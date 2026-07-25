package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	tracer      trace.Tracer
	grpcMetrics *rpcServerMetrics
)

const metricExportInterval = 15 * time.Second

var rpcDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
	0.5, 0.75, 1, 2.5, 5, 7.5, 10,
}

type rpcServerMetrics struct {
	requests otelmetric.Int64Counter
	active   otelmetric.Int64UpDownCounter
	duration otelmetric.Float64Histogram
}

// InitTracer initializes OpenTelemetry traces and metrics. The name is kept
// for source compatibility with existing embedding code.
func InitTracer(serviceName, otelEndpoint string, logger l.Logger) (func(context.Context) error, error) {
	return InitTelemetryWithContext(context.Background(), serviceName, otelEndpoint, logger)
}

// InitTracerWithContext initializes OpenTelemetry traces and metrics using the
// caller's lifecycle. The name is kept for source compatibility.
func InitTracerWithContext(ctx context.Context, serviceName, otelEndpoint string, logger l.Logger) (func(context.Context) error, error) {
	return InitTelemetryWithContext(ctx, serviceName, otelEndpoint, logger)
}

// InitTelemetryWithContext initializes OTLP trace and metric exporters using a
// shared resource and endpoint.
func InitTelemetryWithContext(ctx context.Context, serviceName, otelEndpoint string, logger l.Logger) (func(context.Context) error, error) {
	if otelEndpoint == "" {
		tracer = nil
		grpcMetrics = nil
		logger.Info("OpenTelemetry endpoint not configured, telemetry disabled")
		return nil, nil
	}

	// Create resource with service name.
	version, _, _ := orisun.GetBuildInfo()
	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("service.version", version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	conn, err := grpc.NewClient(
		otelEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP gRPC connection: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			metricExporter,
			sdkmetric.WithInterval(metricExportInterval),
		)),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer = tp.Tracer(serviceName)
	grpcMetrics, err = newRPCServerMetrics(mp.Meter(serviceName))
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("create gRPC metrics: %w", err),
			mp.Shutdown(ctx),
			tp.Shutdown(ctx),
			conn.Close(),
		)
	}

	logger.Infof("OpenTelemetry traces and metrics initialized for service: %s, endpoint: %s", serviceName, otelEndpoint)

	return func(shutdownCtx context.Context) error {
		return errors.Join(
			mp.Shutdown(shutdownCtx),
			tp.Shutdown(shutdownCtx),
			conn.Close(),
		)
	}, nil
}

func newRPCServerMetrics(meter otelmetric.Meter) (*rpcServerMetrics, error) {
	requests, err := meter.Int64Counter(
		"orisun.rpc.server.requests",
		otelmetric.WithDescription("Number of completed RPC server calls."),
		otelmetric.WithUnit("{call}"),
	)
	if err != nil {
		return nil, err
	}
	active, err := meter.Int64UpDownCounter(
		"orisun.rpc.server.active_requests",
		otelmetric.WithDescription("Number of RPC server calls currently in flight."),
		otelmetric.WithUnit("{call}"),
	)
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		"rpc.server.call.duration",
		otelmetric.WithDescription("Duration of an incoming RPC server call."),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(rpcDurationBuckets...),
	)
	if err != nil {
		return nil, err
	}
	return &rpcServerMetrics{
		requests: requests,
		active:   active,
		duration: duration,
	}, nil
}

// GetTracer returns the configured tracer
func GetTracer() trace.Tracer {
	if tracer == nil {
		return otel.Tracer("")
	}
	return tracer
}

// UnaryMetricsInterceptor records OpenTelemetry metrics for unary gRPC calls.
func UnaryMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		metrics := grpcMetrics
		if metrics == nil {
			return handler(ctx, req)
		}
		started := time.Now()
		base := rpcAttributes(info.FullMethod, "unary")
		metrics.active.Add(ctx, 1, otelmetric.WithAttributes(base...))
		defer metrics.active.Add(ctx, -1, otelmetric.WithAttributes(base...))

		response, err := handler(ctx, req)
		metrics.record(ctx, base, started, err)
		return response, err
	}
}

// StreamMetricsInterceptor records OpenTelemetry metrics for streaming gRPC
// calls from stream establishment until the handler returns.
func StreamMetricsInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		metrics := grpcMetrics
		if metrics == nil {
			return handler(srv, ss)
		}
		started := time.Now()
		base := rpcAttributes(info.FullMethod, rpcStreamType(info))
		ctx := ss.Context()
		metrics.active.Add(ctx, 1, otelmetric.WithAttributes(base...))
		defer metrics.active.Add(ctx, -1, otelmetric.WithAttributes(base...))

		err := handler(srv, ss)
		metrics.record(ctx, base, started, err)
		return err
	}
}

func (m *rpcServerMetrics) record(ctx context.Context, base []attribute.KeyValue, started time.Time, err error) {
	code := grpcStatusCode(status.Code(err))
	attributes := append(
		append(make([]attribute.KeyValue, 0, len(base)+2), base...),
		attribute.String("rpc.response.status_code", code),
	)
	if err != nil {
		attributes = append(attributes, attribute.String("error.type", code))
	}
	options := otelmetric.WithAttributes(attributes...)
	m.requests.Add(ctx, 1, options)
	m.duration.Record(ctx, time.Since(started).Seconds(), options)
}

func grpcStatusCode(code grpccodes.Code) string {
	switch code {
	case grpccodes.OK:
		return "OK"
	case grpccodes.Canceled:
		return "CANCELLED"
	case grpccodes.Unknown:
		return "UNKNOWN"
	case grpccodes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case grpccodes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case grpccodes.NotFound:
		return "NOT_FOUND"
	case grpccodes.AlreadyExists:
		return "ALREADY_EXISTS"
	case grpccodes.PermissionDenied:
		return "PERMISSION_DENIED"
	case grpccodes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case grpccodes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case grpccodes.Aborted:
		return "ABORTED"
	case grpccodes.OutOfRange:
		return "OUT_OF_RANGE"
	case grpccodes.Unimplemented:
		return "UNIMPLEMENTED"
	case grpccodes.Internal:
		return "INTERNAL"
	case grpccodes.Unavailable:
		return "UNAVAILABLE"
	case grpccodes.DataLoss:
		return "DATA_LOSS"
	case grpccodes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return "UNKNOWN"
	}
}

func rpcAttributes(fullMethod, rpcType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("rpc.system.name", "grpc"),
		attribute.String("rpc.method", strings.TrimPrefix(fullMethod, "/")),
		attribute.String("orisun.rpc.type", rpcType),
	}
}

func rpcStreamType(info *grpc.StreamServerInfo) string {
	switch {
	case info.IsClientStream && info.IsServerStream:
		return "bidi_stream"
	case info.IsClientStream:
		return "client_stream"
	case info.IsServerStream:
		return "server_stream"
	default:
		return "stream"
	}
}

// UnaryTracingInterceptor returns a gRPC unary interceptor that adds OpenTelemetry tracing
func UnaryTracingInterceptor(logger l.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if tracer == nil {
			// Tracing not enabled, just call handler
			return handler(ctx, req)
		}

		method := strings.TrimPrefix(info.FullMethod, "/")
		ctx, span := tracer.Start(
			ctx,
			method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system.name", "grpc"),
				attribute.String("rpc.method", method),
			),
		)
		defer span.End()

		// Call handler
		resp, err := handler(ctx, req)

		// Set span status based on error
		if err != nil {
			s, _ := status.FromError(err)
			code := grpcStatusCode(s.Code())
			span.SetAttributes(
				attribute.String("rpc.response.status_code", code),
				attribute.String("error.type", code),
			)
			span.SetStatus(otelcodes.Error, s.Message())
		} else {
			span.SetAttributes(attribute.String("rpc.response.status_code", "OK"))
			span.SetStatus(otelcodes.Ok, "")
		}

		return resp, err
	}
}

// StreamTracingInterceptor returns a gRPC streaming interceptor that adds OpenTelemetry tracing
func StreamTracingInterceptor(logger l.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if tracer == nil {
			// Tracing not enabled, just call handler
			return handler(srv, ss)
		}

		// Wrap server stream to capture context
		wrapped := &tracedServerStream{
			ServerStream: ss,
			ctx:          ss.Context(),
		}

		method := strings.TrimPrefix(info.FullMethod, "/")
		ctx, span := tracer.Start(
			wrapped.ctx,
			method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system.name", "grpc"),
				attribute.String("rpc.method", method),
			),
		)
		defer span.End()

		wrapped.ctx = ctx

		// Call handler
		err := handler(srv, wrapped)

		// Set span status based on error
		if err != nil {
			s, _ := status.FromError(err)
			code := grpcStatusCode(s.Code())
			span.SetAttributes(
				attribute.String("rpc.response.status_code", code),
				attribute.String("error.type", code),
			)
			span.SetStatus(otelcodes.Error, s.Message())
		} else {
			span.SetAttributes(attribute.String("rpc.response.status_code", "OK"))
			span.SetStatus(otelcodes.Ok, "")
		}

		return err
	}
}

// tracedServerStream wraps grpc.ServerStream to use the traced context
type tracedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the traced context
func (t *tracedServerStream) Context() context.Context {
	return t.ctx
}
