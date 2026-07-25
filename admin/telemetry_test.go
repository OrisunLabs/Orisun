package admin

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	metricscollectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracecollectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTelemetryExportsTracesAndMetricsOverOTLP(t *testing.T) {
	defer func() {
		tracer = nil
		grpcMetrics = nil
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	collector := grpc.NewServer()
	var traceExports atomic.Int32
	var metricExports atomic.Int32
	tracecollectorpb.RegisterTraceServiceServer(collector, &testTraceCollector{exports: &traceExports})
	metricscollectorpb.RegisterMetricsServiceServer(collector, &testMetricCollector{exports: &metricExports})
	go func() { _ = collector.Serve(listener) }()
	defer collector.Stop()

	shutdown, err := InitTelemetryWithContext(
		t.Context(),
		"orisun-test",
		listener.Addr().String(),
		telemetryTestLogger{},
	)
	if err != nil {
		t.Fatalf("InitTelemetryWithContext() error = %v", err)
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/orisun.EventStore/Ping"}
	_, err = UnaryMetricsInterceptor()(
		t.Context(),
		nil,
		info,
		func(ctx context.Context, req any) (any, error) {
			return UnaryTracingInterceptor(telemetryTestLogger{})(
				ctx,
				req,
				info,
				func(context.Context, any) (any, error) { return nil, nil },
			)
		},
	)
	if err != nil {
		t.Fatalf("instrumented call error = %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatalf("telemetry shutdown error = %v", err)
	}
	if traceExports.Load() == 0 {
		t.Fatal("OTLP collector received no traces")
	}
	if metricExports.Load() == 0 {
		t.Fatal("OTLP collector received no metrics")
	}
}

func TestUnaryMetricsInterceptorRecordsCountDurationAndStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(t.Context()) }()

	metrics, err := newRPCServerMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatalf("newRPCServerMetrics() error = %v", err)
	}
	previous := grpcMetrics
	grpcMetrics = metrics
	defer func() { grpcMetrics = previous }()

	wantErr := status.Error(codes.AlreadyExists, "consistency conflict")
	_, err = UnaryMetricsInterceptor()(
		t.Context(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/orisun.EventStore/SaveEvents"},
		func(context.Context, any) (any, error) {
			return nil, wantErr
		},
	)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("interceptor error = %v", err)
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	requests := metricByName(t, collected, "orisun.rpc.server.requests")
	requestData, ok := requests.Data.(metricdata.Sum[int64])
	if !ok || len(requestData.DataPoints) != 1 || requestData.DataPoints[0].Value != 1 {
		t.Fatalf("request metric = %#v", requests.Data)
	}
	assertMetricAttribute(t, requestData.DataPoints[0].Attributes, "rpc.system.name", "grpc")
	assertMetricAttribute(t, requestData.DataPoints[0].Attributes, "rpc.method", "orisun.EventStore/SaveEvents")
	assertMetricAttribute(t, requestData.DataPoints[0].Attributes, "rpc.response.status_code", "ALREADY_EXISTS")
	assertMetricAttribute(t, requestData.DataPoints[0].Attributes, "error.type", "ALREADY_EXISTS")

	duration := metricByName(t, collected, "rpc.server.call.duration")
	durationData, ok := duration.Data.(metricdata.Histogram[float64])
	if !ok || len(durationData.DataPoints) != 1 || durationData.DataPoints[0].Count != 1 {
		t.Fatalf("duration metric = %#v", duration.Data)
	}
	if len(durationData.DataPoints[0].Bounds) != len(rpcDurationBuckets) {
		t.Fatalf("duration bounds = %v", durationData.DataPoints[0].Bounds)
	}

	active := metricByName(t, collected, "orisun.rpc.server.active_requests")
	activeData, ok := active.Data.(metricdata.Sum[int64])
	if !ok || len(activeData.DataPoints) != 1 || activeData.DataPoints[0].Value != 0 {
		t.Fatalf("active metric = %#v", active.Data)
	}
}

type testTraceCollector struct {
	tracecollectorpb.UnimplementedTraceServiceServer
	exports *atomic.Int32
}

func (c *testTraceCollector) Export(
	context.Context,
	*tracecollectorpb.ExportTraceServiceRequest,
) (*tracecollectorpb.ExportTraceServiceResponse, error) {
	c.exports.Add(1)
	return &tracecollectorpb.ExportTraceServiceResponse{}, nil
}

type testMetricCollector struct {
	metricscollectorpb.UnimplementedMetricsServiceServer
	exports *atomic.Int32
}

func (c *testMetricCollector) Export(
	context.Context,
	*metricscollectorpb.ExportMetricsServiceRequest,
) (*metricscollectorpb.ExportMetricsServiceResponse, error) {
	c.exports.Add(1)
	return &metricscollectorpb.ExportMetricsServiceResponse{}, nil
}

type telemetryTestLogger struct{}

func (telemetryTestLogger) IsDebugEnabled() bool  { return false }
func (telemetryTestLogger) Debug(...any)          {}
func (telemetryTestLogger) Debugf(string, ...any) {}
func (telemetryTestLogger) Info(...any)           {}
func (telemetryTestLogger) Infof(string, ...any)  {}
func (telemetryTestLogger) Warn(...any)           {}
func (telemetryTestLogger) Warnf(string, ...any)  {}
func (telemetryTestLogger) Error(...any)          {}
func (telemetryTestLogger) Errorf(string, ...any) {}
func (telemetryTestLogger) Fatal(...any)          {}
func (telemetryTestLogger) Fatalf(string, ...any) {}

func TestRPCStreamType(t *testing.T) {
	tests := []struct {
		name string
		info grpc.StreamServerInfo
		want string
	}{
		{name: "server", info: grpc.StreamServerInfo{IsServerStream: true}, want: "server_stream"},
		{name: "client", info: grpc.StreamServerInfo{IsClientStream: true}, want: "client_stream"},
		{name: "bidi", info: grpc.StreamServerInfo{IsClientStream: true, IsServerStream: true}, want: "bidi_stream"},
		{name: "unknown", info: grpc.StreamServerInfo{}, want: "stream"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := rpcStreamType(&test.info); got != test.want {
				t.Fatalf("rpcStreamType() = %q, want %q", got, test.want)
			}
		})
	}
}

func metricByName(t *testing.T, collected metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func assertMetricAttribute(t *testing.T, attributes attribute.Set, key, want string) {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(key))
	if !ok || value.AsString() != want {
		t.Fatalf("attribute %q = %q, want %q", key, value.AsString(), want)
	}
}
