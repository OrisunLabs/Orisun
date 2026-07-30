package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const defaultScenarios = "write-conditional-1,write-conditional-100,write-unconditional-1,read-conditional,read-unconditional,write-unconditional-1-with-readers"

type benchmarkConfig struct {
	Address             string
	Boundary            string
	Backend             string
	Namespace           string
	Username            string
	Password            string
	AuthMode            string
	ScenarioNames       string
	ConcurrencyValues   string
	Duration            time.Duration
	Warmup              time.Duration
	Timeout             time.Duration
	PrepopulateEvents   int
	TagCardinality      int
	PrepopulateBatch    int
	ReadBatch           int
	ConstrainedReadRate int
	OppositeClients     int
	PayloadBytes        int
	OutputDir           string
	CreateBoundary      bool
	CreateIndex         bool
	Smoke               bool
	Labels              labelsFlag
}

type labelsFlag map[string]string

func (labels labelsFlag) String() string {
	parts := make([]string, 0, len(labels))
	for key, value := range labels {
		parts = append(parts, key+"="+value)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (labels labelsFlag) Set(value string) error {
	key, labelValue, found := strings.Cut(value, "=")
	key = strings.TrimSpace(key)
	labelValue = strings.TrimSpace(labelValue)
	if !found || key == "" || labelValue == "" {
		return errors.New("label must use non-empty key=value syntax")
	}
	labels[key] = labelValue
	return nil
}

type benchmarkClient struct {
	config     benchmarkConfig
	conn       *grpc.ClientConn
	eventStore grpcapi.EventStoreClient
	admin      grpcapi.AdminClient
	baseCtx    context.Context
	serverInfo *grpcapi.GetServerInfoResponse
	ids        idSource
}

func main() {
	if err := run(parseFlags()); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() benchmarkConfig {
	config := benchmarkConfig{Labels: make(labelsFlag)}
	flag.StringVar(&config.Address, "address", "127.0.0.1:5005", "Orisun gRPC address")
	flag.StringVar(&config.Boundary, "boundary", "benchmark", "benchmark boundary")
	flag.StringVar(&config.Backend, "backend", "postgres", "boundary backend when --create-boundary is set")
	flag.StringVar(&config.Namespace, "namespace", "benchmark", "boundary namespace when --create-boundary is set")
	flag.StringVar(&config.Username, "username", "admin", "basic-auth username")
	flag.StringVar(&config.Password, "password", "changeit", "basic-auth password")
	flag.StringVar(&config.AuthMode, "auth", "token", "request authentication: token or basic")
	flag.StringVar(&config.ScenarioNames, "scenarios", defaultScenarios, "comma-separated scenario names, or all")
	flag.StringVar(&config.ConcurrencyValues, "concurrency", "1,4,16,64,256", "comma-separated concurrent client counts")
	flag.DurationVar(&config.Duration, "duration", 30*time.Second, "measurement time per point")
	flag.DurationVar(&config.Warmup, "warmup", 5*time.Second, "warm-up time per point")
	flag.DurationVar(&config.Timeout, "timeout", 15*time.Second, "individual gRPC request timeout")
	flag.IntVar(&config.PrepopulateEvents, "prepopulate-events", 1_000_000, "events loaded before measurement")
	flag.IntVar(&config.TagCardinality, "tag-cardinality", 100_000, "distinct benchmark_context values")
	flag.IntVar(&config.PrepopulateBatch, "prepopulate-batch", 1_000, "events per prepopulation request")
	flag.IntVar(&config.ReadBatch, "read-batch", 1_000, "maximum events per read request")
	flag.IntVar(&config.ConstrainedReadRate, "constrained-read-rate", 10_000, "target events/second per constrained reader")
	flag.IntVar(&config.OppositeClients, "opposite-clients", 4, "background clients in mixed workloads")
	flag.IntVar(&config.PayloadBytes, "payload-bytes", 256, "approximate event data size")
	flag.StringVar(&config.OutputDir, "output", "benchmark-results", "parent result directory")
	flag.BoolVar(&config.CreateBoundary, "create-boundary", false, "create a clean benchmark boundary")
	flag.BoolVar(&config.CreateIndex, "create-index", true, "create the benchmark_context index")
	flag.BoolVar(&config.Smoke, "smoke", false, "run a short harness check; results are not publishable")
	flag.Var(config.Labels, "label", "result metadata in key=value form; may be repeated")
	flag.Parse()

	if config.Smoke {
		config.ScenarioNames = "write-conditional-1,read-conditional"
		config.ConcurrencyValues = "1,4"
		config.Duration = time.Second
		config.Warmup = 250 * time.Millisecond
		if config.PrepopulateEvents == 1_000_000 {
			config.PrepopulateEvents = 1_000
		}
		if config.TagCardinality == 100_000 {
			config.TagCardinality = 100
		}
	}
	return config
}

func run(config benchmarkConfig) error {
	if err := validateConfig(config); err != nil {
		return err
	}
	concurrency, err := parsePositiveInts(config.ConcurrencyValues)
	if err != nil {
		return fmt.Errorf("concurrency: %w", err)
	}
	scenarios, err := selectScenarios(config.ScenarioNames)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := connect(ctx, config)
	if err != nil {
		return err
	}
	defer client.conn.Close()
	if err := validateBuiltArtifacts(client.serverInfo); err != nil {
		return err
	}

	if err := client.prepareBoundary(ctx); err != nil {
		return err
	}
	report, err := newReport(config, concurrency, scenarios, client.serverInfo)
	if err != nil {
		return err
	}
	fmt.Printf("results: %s\n", report.runDir)

	for _, workload := range scenarios {
		for _, clients := range concurrency {
			fmt.Printf("%-44s clients=%-4d ", workload.Name, clients)
			result := client.measureScenario(ctx, workload, clients)
			report.Results = append(report.Results, result)
			fmt.Printf(
				"%10.0f events/s  p50=%7.2fms  p99=%7.2fms  errors=%d\n",
				result.EventsPerSecond,
				float64(result.LatencyP50NS)/1e6,
				float64(result.LatencyP99NS)/1e6,
				result.Errors+result.BackgroundErrors,
			)
			if err := report.write(false); err != nil {
				return err
			}
		}
	}

	report.Completed = true
	report.CompletedAt = time.Now().UTC()
	if err := report.write(true); err != nil {
		return err
	}
	if errors := report.totalErrors(); errors > 0 {
		return fmt.Errorf("benchmark completed with %d request errors; see %s", errors, report.runDir)
	}
	return nil
}

func validateBuiltArtifacts(server *grpcapi.GetServerInfoResponse) error {
	var issues []string
	if benchmarkVersion == "" || benchmarkVersion == "dev" ||
		benchmarkGitCommit == "" || benchmarkGitCommit == "unknown" ||
		benchmarkBuildTime == "" || benchmarkBuildTime == "unknown" {
		issues = append(issues, "benchmark runner has development build metadata")
	}
	if server == nil {
		issues = append(issues, "server did not report build metadata")
	} else if server.Version == "" || server.Version == "dev" ||
		server.GitCommit == "" || server.GitCommit == "unknown" ||
		server.BuildTime == "" || server.BuildTime == "unknown" {
		issues = append(issues, "server has development build metadata")
	}
	if len(issues) > 0 {
		return fmt.Errorf(
			"%s; build and run binaries before benchmarking (do not use go run)",
			strings.Join(issues, "; "),
		)
	}
	return nil
}

func validateConfig(config benchmarkConfig) error {
	switch {
	case config.Duration <= 0:
		return errors.New("duration must be positive")
	case config.Warmup < 0:
		return errors.New("warmup must not be negative")
	case config.Timeout <= 0:
		return errors.New("timeout must be positive")
	case config.PrepopulateEvents < 0:
		return errors.New("prepopulate-events must not be negative")
	case config.TagCardinality <= 0:
		return errors.New("tag-cardinality must be positive")
	case config.PrepopulateBatch <= 0:
		return errors.New("prepopulate-batch must be positive")
	case config.ReadBatch <= 0 || config.ReadBatch > 10_000:
		return errors.New("read-batch must be between 1 and 10000")
	case config.ConstrainedReadRate <= 0:
		return errors.New("constrained-read-rate must be positive")
	case config.OppositeClients <= 0:
		return errors.New("opposite-clients must be positive")
	case config.PayloadBytes < 64:
		return errors.New("payload-bytes must be at least 64")
	case config.AuthMode != "token" && config.AuthMode != "basic":
		return errors.New("auth must be token or basic")
	case config.CreateBoundary && strings.TrimSpace(config.Namespace) == "":
		return errors.New("namespace is required with create-boundary")
	}
	return nil
}

func (c *benchmarkClient) prepareBoundary(ctx context.Context) error {
	if c.config.CreateBoundary {
		if err := c.ensureBoundary(ctx); err != nil {
			return err
		}
	}
	if c.config.CreateIndex {
		if err := c.ensureIndex(ctx); err != nil {
			return err
		}
	}
	if c.config.PrepopulateEvents > 0 {
		if err := c.prepopulate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func parsePositiveInts(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	result := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		number, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || number <= 0 {
			return nil, fmt.Errorf("%q is not a positive integer", part)
		}
		if _, exists := seen[number]; exists {
			continue
		}
		seen[number] = struct{}{}
		result = append(result, number)
	}
	if len(result) == 0 {
		return nil, errors.New("at least one value is required")
	}
	return result, nil
}

func connect(ctx context.Context, config benchmarkConfig) (*benchmarkClient, error) {
	conn, err := grpc.DialContext(
		ctx,
		config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
		grpc.WithInitialWindowSize(1024*1024),
		grpc.WithInitialConnWindowSize(1024*1024),
		grpc.WithWriteBufferSize(64*1024),
		grpc.WithReadBufferSize(64*1024),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", config.Address, err)
	}
	client := &benchmarkClient{
		config:     config,
		conn:       conn,
		eventStore: grpcapi.NewEventStoreClient(conn),
		admin:      grpcapi.NewAdminClient(conn),
		ids:        idSource{next: uint64(time.Now().UnixNano())},
	}
	basic := base64.StdEncoding.EncodeToString([]byte(config.Username + ":" + config.Password))
	basicCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+basic)
	client.baseCtx = basicCtx
	if config.AuthMode == "token" {
		var headers metadata.MD
		pingCtx, cancel := context.WithTimeout(basicCtx, config.Timeout)
		_, pingErr := client.eventStore.Ping(pingCtx, &grpcapi.PingRequest{}, grpc.Header(&headers))
		cancel()
		if pingErr != nil {
			conn.Close()
			return nil, fmt.Errorf("authenticate: %w", pingErr)
		}
		tokens := headers.Get("x-auth-token")
		if len(tokens) == 0 {
			conn.Close()
			return nil, errors.New("authenticate: server returned no x-auth-token")
		}
		client.baseCtx = metadata.AppendToOutgoingContext(ctx, "x-auth-token", tokens[0])
	}
	infoCtx, infoCancel := context.WithTimeout(client.baseCtx, config.Timeout)
	client.serverInfo, err = client.eventStore.GetServerInfo(infoCtx, &grpcapi.GetServerInfoRequest{})
	infoCancel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("get server info: %w", err)
	}
	return client, nil
}

func (c *benchmarkClient) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	outgoing, _ := metadata.FromOutgoingContext(c.baseCtx)
	return context.WithTimeout(metadata.NewOutgoingContext(parent, outgoing), c.config.Timeout)
}

func (c *benchmarkClient) ensureBoundary(ctx context.Context) error {
	requestCtx, cancel := c.requestContext(ctx)
	_, err := c.admin.CreateBoundary(requestCtx, &grpcapi.CreateBoundaryRequest{
		Name: c.config.Boundary,
		Placement: &grpcapi.BoundaryPlacementInput{
			Backend:   c.config.Backend,
			Namespace: c.config.Namespace,
		},
	})
	cancel()
	if err != nil {
		return fmt.Errorf("create clean boundary %s: %w", c.config.Boundary, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		requestCtx, cancel = c.requestContext(ctx)
		response, getErr := c.admin.GetBoundary(requestCtx, &grpcapi.GetBoundaryRequest{Name: c.config.Boundary})
		cancel()
		if getErr == nil && response.Boundary != nil &&
			response.Boundary.Status == grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("boundary %s did not become active", c.config.Boundary)
}

func (c *benchmarkClient) ensureIndex(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		requestCtx, cancel := c.requestContext(ctx)
		_, err := c.eventStore.CreateIndex(requestCtx, &grpcapi.CreateIndexRequest{
			Boundary: c.config.Boundary,
			Name:     "benchmark_context",
			Fields: []*grpcapi.IndexField{{
				JsonKey:   "benchmark_context",
				ValueType: grpcapi.ValueType_TEXT,
			}},
		})
		cancel()
		if err == nil || status.Code(err) == codes.AlreadyExists {
			return nil
		}
		if !boundaryRuntimeStarting(err) || time.Now().After(deadline) {
			return fmt.Errorf("create benchmark index: %w", err)
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func boundaryRuntimeStarting(err error) bool {
	code := status.Code(err)
	message := strings.ToLower(status.Convert(err).Message())
	return (code == codes.FailedPrecondition || code == codes.NotFound) &&
		strings.Contains(message, "boundary") &&
		(strings.Contains(message, "not active") || strings.Contains(message, "unknown"))
}

func environmentSummary() map[string]string {
	hostname, _ := os.Hostname()
	return map[string]string{
		"hostname":     hostname,
		"os":           runtime.GOOS,
		"architecture": runtime.GOARCH,
		"go_version":   runtime.Version(),
		"gomaxprocs":   strconv.Itoa(runtime.GOMAXPROCS(0)),
		"cpu_count":    strconv.Itoa(runtime.NumCPU()),
	}
}
