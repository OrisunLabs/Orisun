package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/OrisunLabs/Orisun/postgres"
)

type BenchmarkSetup struct {
	backend           string // "postgres" or "sqlite", from ORISUN_BENCH_BACKEND
	postgresContainer testcontainers.Container
	binaryPath        string
	binaryCmd         *exec.Cmd
	client            grpcapi.EventStoreClient
	conn              *grpc.ClientConn
	ctx               context.Context
	cancel            context.CancelFunc
	postgresHost      string
	postgresPort      string
	sqliteDir         string
	grpcPort          string
	adminPort         string
}

type benchmarkGRPCClient struct {
	client grpcapi.EventStoreClient
	ctx    context.Context
}

const (
	benchmarkGRPCMaxMessageSize  = 100 * 1024 * 1024
	benchmarkGRPCWindowSize      = 1024 * 1024
	benchmarkGRPCWriteBufferSize = 65536
	benchmarkGRPCReadBufferSize  = 65536
	benchmarkRecommendedInFlight = 1024
)

func benchmarkAuthTokenInterceptor() grpc.UnaryClientInterceptor {
	var token atomic.Value

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if v := token.Load(); v != nil {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-auth-token", v.(string))
		}

		var responseMD metadata.MD
		var responseTrailer metadata.MD
		opts = append(opts, grpc.Header(&responseMD), grpc.Trailer(&responseTrailer))

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			if tokens := responseMD.Get("x-auth-token"); len(tokens) > 0 {
				token.Store(tokens[0])
			}
		}
		return err
	}
}

// benchBackend reads ORISUN_BENCH_BACKEND, defaulting to "postgres".
// Set ORISUN_BENCH_BACKEND=sqlite to run gRPC benchmarks against the sqlite backend.
func benchBackend() string {
	v := os.Getenv("ORISUN_BENCH_BACKEND")
	if v == "" {
		return "postgres"
	}
	return v
}

func setupBenchmark(b *testing.B) *BenchmarkSetup {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	setup := &BenchmarkSetup{
		backend:   benchBackend(),
		ctx:       ctx,
		cancel:    cancel,
		grpcPort:  "15006", // Use different port for benchmarks
		adminPort: "18992",
	}

	b.Logf("benchmark backend: %s", setup.backend)

	if setup.backend == "sqlite" {
		setup.sqliteDir = b.TempDir()
		setup.buildBinary(b)
		setup.startBinary(b)
		setup.waitForGRPCServer(b)
		setup.createGRPCClient(b)
		setup.createBenchmarkBoundaries(b)
		return setup
	}

	// Start PostgreSQL container with optimized configuration
	req := testcontainers.ContainerRequest{
		Image:        "postgres:17.5-alpine3.22",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "orisun",
		},
		Cmd: []string{
			"postgres",
			// Memory settings
			//"-c", "shared_buffers=2GB",
			//"-c", "work_mem=128MB",
			//"-c", "maintenance_work_mem=512MB",
			//"-c", "effective_cache_size=6GB",
			//// WAL settings for write performance
			//"-c", "wal_buffers=128MB",
			//"-c", "min_wal_size=2GB",
			//"-c", "max_wal_size=8GB",
			//"-c", "checkpoint_completion_target=0.7",
			//"-c", "checkpoint_timeout=3min",
			//// Connection and concurrency
			"-c", "max_connections=300",
			//"-c", "max_worker_processes=20",
			//"-c", "max_parallel_workers=14",
			//"-c", "max_parallel_workers_per_gather=7",
			//"-c", "max_parallel_maintenance_workers=7",
			//// Performance tuning
			//"-c", "random_page_cost=1.1",
			//"-c", "seq_page_cost=1.0",
			//"-c", "cpu_tuple_cost=0.01",
			//"-c", "cpu_index_tuple_cost=0.005",
			//"-c", "cpu_operator_cost=0.0025",
			//// Logging for performance monitoring
			//"-c", "log_min_duration_statement=1000",
			//"-c", "log_checkpoints=on",
			//"-c", "log_connections=on",
			//"-c", "log_disconnections=on",
			//"-c", "log_lock_waits=on",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(b, err)

	// Store the container
	setup.postgresContainer = container

	// Get PostgreSQL connection details
	host, err := container.Host(ctx)
	require.NoError(b, err)
	setup.postgresHost = host

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(b, err)
	setup.postgresPort = port.Port()

	// Build the binary
	setup.buildBinary(b)

	// Start the binary with proper environment variables
	setup.startBinary(b)

	// Wait for the gRPC server to be ready
	setup.waitForGRPCServer(b)

	// Create gRPC client
	setup.createGRPCClient(b)
	setup.createBenchmarkBoundaries(b)

	return setup
}

func (s *BenchmarkSetup) buildBinary(b *testing.B) {
	// Determine the target OS and architecture
	targetOS := runtime.GOOS
	targetArch := runtime.GOARCH

	// Create build directory if it doesn't exist
	buildDir := "./build"
	err := os.MkdirAll(buildDir, 0755)
	require.NoError(b, err)

	// Set binary name
	binaryName := fmt.Sprintf("orisun-%s-%s", targetOS, targetArch)
	s.binaryPath = filepath.Join(buildDir, binaryName)

	// Build the binary using the same command as build.sh
	cmd := exec.Command("go", "build", "-ldflags", "-s -w", "-o", s.binaryPath, ".")
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	b.Logf("Building binary: %s", s.binaryPath)
	err = cmd.Run()
	require.NoError(b, err, "Failed to build binary")

	// Verify binary exists
	_, err = os.Stat(s.binaryPath)
	require.NoError(b, err, "Binary not found after build")
}

func (s *BenchmarkSetup) startBinary(b *testing.B) {
	// Common env (boundaries, NATS, gRPC, admin) shared across backends
	env := []string{
		fmt.Sprintf("ORISUN_BACKEND=%s", s.backend),
		"ORISUN_PPROF_ENABLED=false",
		"ORISUN_OTEL_ENABLED=false",
		"ORISUN_PPROF_PORT=6060",
		fmt.Sprintf("ORISUN_GRPC_PORT=%s", s.grpcPort),
		fmt.Sprintf("ORISUN_ADMIN_PORT=%s", s.adminPort),
		"ORISUN_GRPC_ENABLE_REFLECTION=false",
		"ORISUN_NATS_PORT=14224",
		"ORISUN_NATS_CLUSTER_PORT=16222",
		"ORISUN_NATS_CLUSTER_ENABLED=false",
		"ORISUN_LOGGING_LEVEL=WARN",
		"ORISUN_ADMIN_USERNAME=admin",
		"ORISUN_ADMIN_PASSWORD=changeit",
		"ORISUN_ADMIN_BOUNDARY=benchmark_admin",
	}

	switch s.backend {
	case "postgres":
		env = append(env,
			fmt.Sprintf("ORISUN_PG_HOST=%s", s.postgresHost),
			fmt.Sprintf("ORISUN_PG_PORT=%s", s.postgresPort),
			"ORISUN_PG_USER=postgres",
			"ORISUN_PG_PASSWORD=postgres",
			"ORISUN_PG_NAME=orisun",
			"ORISUN_PG_ADMIN_SCHEMA=admin",
			"ORISUN_PG_WRITE_MAX_OPEN_CONNS=200",
			"ORISUN_PG_WRITE_MAX_IDLE_CONNS=20",
			"ORISUN_PG_READ_MAX_OPEN_CONNS=10",
			"ORISUN_PG_READ_MAX_IDLE_CONNS=5",
		)
	case "sqlite":
		env = append(env,
			fmt.Sprintf("ORISUN_SQLITE_DIR=%s", s.sqliteDir),
		)
	case "foundationdb":
		env = append(env,
			fmt.Sprintf("ORISUN_SQLITE_DIR=%s", s.sqliteDir),
		)
	default:
		b.Fatalf("unknown benchmark backend: %s", s.backend)
	}

	// Add current environment variables
	env = append(env, os.Environ()...)

	// Start the binary
	s.binaryCmd = exec.Command(s.binaryPath)
	s.binaryCmd.Env = env
	// Capture output for debugging
	s.binaryCmd.Stdout = os.Stdout
	s.binaryCmd.Stderr = os.Stderr
	s.binaryCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	b.Logf("Starting binary: %s", s.binaryPath)
	err := s.binaryCmd.Start()
	require.NoError(b, err, "Failed to start binary")

	b.Logf("Binary started with PID: %d", s.binaryCmd.Process.Pid)

	// Give the binary some time to start
	time.Sleep(3 * time.Second)
}

func (s *BenchmarkSetup) waitForGRPCServer(b *testing.B) {
	// Wait for gRPC server to be ready
	for i := range 60 { // Increase timeout to 60 seconds
		conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%s", s.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			conn.Close()
			b.Logf("gRPC server is ready on port %s", s.grpcPort)
			// Add extra delay to ensure server is fully ready
			time.Sleep(2 * time.Second)
			return
		}
		b.Logf("Attempt %d: gRPC server not ready yet: %v", i+1, err)
		time.Sleep(1 * time.Second)
	}
	require.Fail(b, "gRPC server did not start within 60 seconds")
}

func (s *BenchmarkSetup) createGRPCClient(b *testing.B) {
	conn, client := s.createGRPCEventStoreClient(b, true)
	s.conn = conn
	s.client = client

	// Wait for admin user to be created by the projector.
	s.authenticateGRPCClient(b, s.client)
}

func (s *BenchmarkSetup) createBenchmarkBoundaries(b *testing.B) {
	b.Helper()
	adminClient := grpcapi.NewAdminClient(s.conn)
	for _, name := range []string{"benchmark_test", "subscribe_boundary"} {
		namespace := "public"
		if s.backend == "sqlite" {
			namespace = name
		}
		_, err := adminClient.CreateBoundary(s.authContext(), &grpcapi.CreateBoundaryRequest{
			Name:      name,
			Placement: &grpcapi.BoundaryPlacementInput{Backend: s.backend, Namespace: namespace},
		})
		require.NoError(b, err)
		require.Eventually(b, func() bool {
			response, getErr := adminClient.GetBoundary(s.authContext(), &grpcapi.GetBoundaryRequest{Name: name})
			return getErr == nil &&
				response.Boundary != nil &&
				response.Boundary.Status == grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE
		}, 10*time.Second, 25*time.Millisecond)
	}
}

func (s *BenchmarkSetup) createGRPCEventStoreClient(b *testing.B, captureAuthToken bool) (*grpc.ClientConn, grpcapi.EventStoreClient) {
	b.Helper()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(benchmarkGRPCMaxMessageSize)),
		grpc.WithInitialWindowSize(benchmarkGRPCWindowSize),
		grpc.WithInitialConnWindowSize(benchmarkGRPCWindowSize),
		grpc.WithWriteBufferSize(benchmarkGRPCWriteBufferSize),
		grpc.WithReadBufferSize(benchmarkGRPCReadBufferSize),
	}
	if captureAuthToken {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(benchmarkAuthTokenInterceptor()))
	}

	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%s", s.grpcPort), dialOptions...)
	require.NoError(b, err)
	return conn, grpcapi.NewEventStoreClient(conn)
}

func (s *BenchmarkSetup) authenticateGRPCClient(b *testing.B, client grpcapi.EventStoreClient) {
	b.Helper()

	var pingResp *grpcapi.PingResponse
	var err error
	for i := range 30 {
		pingResp, err = client.Ping(s.authContext(), &grpcapi.PingRequest{})
		if err == nil {
			b.Logf("Authentication successful on attempt %d", i+1)
			return
		}
		b.Logf("Authentication attempt %d failed: %v, retrying...", i+1, err)
		time.Sleep(time.Second)
	}
	b.Fatalf("Final ping response: %v, %v", pingResp, err)
}

func (s *BenchmarkSetup) authTokenForGRPCClient(b *testing.B, client grpcapi.EventStoreClient) string {
	b.Helper()

	var responseMD metadata.MD
	_, err := client.Ping(s.authContext(), &grpcapi.PingRequest{}, grpc.Header(&responseMD))
	require.NoError(b, err)

	tokens := responseMD.Get("x-auth-token")
	require.NotEmpty(b, tokens)
	return tokens[0]
}

func (s *BenchmarkSetup) tokenAuthContext(token string) context.Context {
	return metadata.AppendToOutgoingContext(s.ctx, "x-auth-token", token)
}

func (s *BenchmarkSetup) createGRPCBenchmarkClients(b *testing.B, count int, authMode string) ([]*grpc.ClientConn, []benchmarkGRPCClient) {
	b.Helper()

	conns := make([]*grpc.ClientConn, 0, count)
	clients := make([]benchmarkGRPCClient, 0, count)
	for range count {
		useAuthTokenInterceptor := authMode == "rolling_token_interceptor"
		conn, client := s.createGRPCEventStoreClient(b, useAuthTokenInterceptor)
		conns = append(conns, conn)

		switch authMode {
		case "rolling_token_interceptor":
			s.authenticateGRPCClient(b, client)
			clients = append(clients, benchmarkGRPCClient{
				client: client,
				ctx:    s.authContext(),
			})
		case "static_token":
			token := s.authTokenForGRPCClient(b, client)
			clients = append(clients, benchmarkGRPCClient{
				client: client,
				ctx:    s.tokenAuthContext(token),
			})
		default:
			b.Fatalf("unknown benchmark auth mode: %s", authMode)
		}
	}
	return conns, clients
}

func (s *BenchmarkSetup) cleanup(b *testing.B) {
	b.Helper()

	// Close gRPC connection
	if s.conn != nil {
		s.conn.Close()
	}

	// Stop the binary
	if s.binaryCmd != nil && s.binaryCmd.Process != nil {
		b.Logf("Stopping binary with PID: %d", s.binaryCmd.Process.Pid)
		// Send SIGTERM to the process group
		err := syscall.Kill(-s.binaryCmd.Process.Pid, syscall.SIGTERM)
		if err != nil {
			b.Logf("Failed to send SIGTERM: %v", err)
		}

		// Wait for process to exit gracefully
		done := make(chan error, 1)
		go func() {
			done <- s.binaryCmd.Wait()
		}()

		select {
		case <-time.After(10 * time.Second):
			b.Logf("Binary did not exit gracefully, sending SIGKILL")
			syscall.Kill(-s.binaryCmd.Process.Pid, syscall.SIGKILL)
			s.binaryCmd.Wait()
		case <-done:
			b.Logf("Binary exited gracefully")
		}
	}

	// Stop PostgreSQL container (only when PG backend was used)
	if s.postgresContainer != nil {
		err := s.postgresContainer.Terminate(s.ctx)
		if err != nil {
			b.Logf("Failed to terminate postgres container: %v", err)
		}
	}

	// SQLite tempdir is auto-cleaned by t.TempDir; nothing to do here.

	s.cancel()
}

func (s *BenchmarkSetup) authContext() context.Context {
	creds := base64.StdEncoding.EncodeToString([]byte("admin:changeit"))
	return metadata.AppendToOutgoingContext(s.ctx, "authorization", "Basic "+creds)
}

func generateRandomEvent(eventType string) *grpcapi.EventToSave {
	timestamp := time.Now().Format(time.RFC3339)
	return &grpcapi.EventToSave{
		EventId:   uuid.New().String(),
		EventType: eventType,
		Data: fmt.Sprintf(`{
			"timestamp": "%s",
			"message": "random event",
			"user_id": "%s",
			"session_id": "%s",
			"event_id": "%s",
			"source_ip": "192.168.1.100",
			"user_agent": "Mozilla/5.0",
			"request_id": "%s",
			"correlation_id": "%s",
			"priority": "high",
			"status": "active",
			"region": "us-east-1",
			"environment": "production",
			"version": "1.0.0",
			"category": "user_action",
			"sub_category": "login",
			"retry_count": 3,
			"latency_ms": 45,
			"success": true,
			"tags": ["benchmark", "test", "performance"]
		}`, timestamp, uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String()),
		Metadata: `{
			"source": "benchmark",
			"hostname": "test-server",
			"pid": 12345,
			"thread_id": "thread-1",
			"app_version": "2.1.0"
		}`,
	}
}

func generateEvents(count int, streamId string) []*grpcapi.EventToSave {
	timestamp := time.Now().Format(time.RFC3339)
	events := make([]*grpcapi.EventToSave, count)
	for i := range count {
		events[i] = &grpcapi.EventToSave{
			EventId:   uuid.New().String(),
			EventType: "TestEvent",
			Data: fmt.Sprintf(`{
				"timestamp": "%s",
				"message": "test event %d",
				"user_id": "%s",
				"session_id": "%s",
				"event_id": "%s",
				"source_ip": "192.168.1.100",
				"user_agent": "Mozilla/5.0",
				"request_id": "%s",
				"correlation_id": "%s",
				"priority": "high",
				"status": "active",
				"region": "us-east-1",
				"environment": "production",
				"version": "1.0.0",
				"category": "test",
				"sub_category": "benchmark",
				"retry_count": 0,
				"latency_ms": 25,
				"success": true,
				"tags": ["test", "benchmark", "performance"],
				"stream_id": "%s"
			}`, timestamp, i, uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String(), streamId),
			Metadata: fmt.Sprintf(`{
				"source": "benchmark",
				"hostname": "test-server",
				"pid": 12345,
				"thread_id": "thread-1",
				"app_version": "2.1.0",
				"stream": "%s"
			}`, streamId),
		}
	}
	return events
}

// BenchmarkGetEvents tests event retrieval
func BenchmarkGetEvents(b *testing.B) {
	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	// Pre-populate with events
	boundary := "benchmark_test"
	streamId := uuid.New().String()
	events := generateEvents(1000, streamId)
	p := grpcapi.Position{CommitPosition: -1, PreparePosition: -1}
	_, err := setup.client.SaveEvents(setup.authContext(), &grpcapi.SaveEventsRequest{
		Boundary: boundary,
		Query: &grpcapi.SaveQuery{
			ExpectedPosition: &p,
		},
		Events: events,
	})
	if err != nil {
		b.Fatalf("Failed to pre-populate events: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := setup.client.GetEvents(setup.authContext(), &grpcapi.GetEventsRequest{
			Boundary: boundary,
			Count:    50,
		})
		if err != nil {
			b.Errorf("Failed to get events: %v", err)
		}
	}
}

// BenchmarkSubscribeToEvents tests event subscription performance
func BenchmarkSubscribeToEvents(b *testing.B) {
	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	boundary := "subscribe_boundary"

	// Pre-populate with events to ensure subscription has data to read
	for range 100 {
		event := generateRandomEvent("PrePopulateEvent")
		p := grpcapi.Position{CommitPosition: -1, PreparePosition: -1}
		_, err := setup.client.SaveEvents(setup.authContext(), &grpcapi.SaveEventsRequest{
			Boundary: boundary,
			Query: &grpcapi.SaveQuery{
				ExpectedPosition: &p,
			},
			Events: []*grpcapi.EventToSave{event},
		})
		if err != nil {
			b.Fatalf("Failed to pre-populate events: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := setup.client.CatchUpSubscribeToEvents(setup.authContext(), &grpcapi.CatchUpSubscribeToEventStoreRequest{
			Boundary:       boundary,
			SubscriberName: fmt.Sprintf("benchmark_subscriber_%d", i),
		})
		if err != nil {
			b.Errorf("Failed to subscribe to events: %v", err)
			continue
		}

		// Read events from the subscription (should have pre-populated data)
		eventsRead := 0
		for range 10 {
			_, err := stream.Recv()
			if err != nil {
				break
			}
			eventsRead++
		}
		stream.CloseSend()

		// Only count successful subscriptions that read events
		if eventsRead == 0 {
			b.Errorf("Subscription %d read no events", i)
		}
	}
}

// BenchmarkSaveEvents_Burst10000 dispatches 10,000 SaveEvents concurrently at the same moment
func BenchmarkSaveEvents_Burst10000(b *testing.B) {
	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	benchmarkSaveEventsBurst10000(b, []benchmarkGRPCClient{{
		client: setup.client,
		ctx:    setup.authContext(),
	}})
}

func BenchmarkSaveEvents_Burst10000_ClientConns(b *testing.B) {
	for _, authMode := range []string{"rolling_token_interceptor", "static_token"} {
		b.Run(fmt.Sprintf("auth=%s", authMode), func(b *testing.B) {
			setup := setupBenchmark(b)
			defer setup.cleanup(b)

			conns, clients := setup.createGRPCBenchmarkClients(b, 1, authMode)
			defer func() {
				for _, conn := range conns {
					conn.Close()
				}
			}()

			benchmarkSaveEventsBurst10000(b, clients)
		})
	}
}

func benchmarkSaveEventsBurst10000(b *testing.B, clients []benchmarkGRPCClient) {
	benchmarkSaveEventsBurst10000WithMaxInFlight(b, clients, 0)
}

func BenchmarkSaveEvents_Burst10000_ClientInFlight(b *testing.B) {
	for _, maxInFlight := range []int{128, 256, 512, 1024, 2048, 0} {
		name := fmt.Sprintf("max_in_flight=%d", maxInFlight)
		if maxInFlight == 0 {
			name = "max_in_flight=all"
		}
		b.Run(name, func(b *testing.B) {
			setup := setupBenchmark(b)
			defer setup.cleanup(b)

			conns, clients := setup.createGRPCBenchmarkClients(b, 1, "static_token")
			defer func() {
				for _, conn := range conns {
					conn.Close()
				}
			}()

			benchmarkSaveEventsBurst10000WithMaxInFlight(b, clients, maxInFlight)
		})
	}
}

func BenchmarkSaveEvents_Burst10000_TunedClient(b *testing.B) {
	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	conns, clients := setup.createGRPCBenchmarkClients(b, 1, "static_token")
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	benchmarkSaveEventsBurst10000WithMaxInFlight(b, clients, benchmarkRecommendedInFlight)
}

func benchmarkSaveEventsBurst10000WithMaxInFlight(b *testing.B, clients []benchmarkGRPCClient, maxInFlight int) {
	b.Helper()

	boundary := "benchmark_test"
	concurrency := 10000

	var totalEvents int64
	var totalErrors int64

	start := make(chan struct{})

	// Pre-create all events to reduce allocation during timing
	events := make([]*grpcapi.EventToSave, concurrency)
	for i := range concurrency {
		events[i] = generateRandomEvent("BurstEvent")
	}

	var workerWg sync.WaitGroup
	var inFlight chan struct{}
	if maxInFlight > 0 {
		inFlight = make(chan struct{}, maxInFlight)
	}

	// Start worker goroutines
	for w := range events {
		workerWg.Go(func() {
			<-start
			if inFlight != nil {
				inFlight <- struct{}{}
				defer func() { <-inFlight }()
			}
			client := clients[w%len(clients)]
			_, err := client.client.SaveEvents(client.ctx, &grpcapi.SaveEventsRequest{
				Boundary: boundary,
				Events:   []*grpcapi.EventToSave{events[w]},
			})
			if err == nil {
				atomic.AddInt64(&totalEvents, 1)
			} else {
				atomic.AddInt64(&totalErrors, 1)
			}
		})
	}

	// Measure the burst window only
	b.ResetTimer()
	startTime := time.Now()
	close(start) // Signal all workers to start

	workerWg.Wait()
	b.StopTimer()
	elapsed := time.Since(startTime)

	b.ReportMetric(float64(totalEvents)/elapsed.Seconds(), "events/sec")
	if totalErrors > 0 {
		b.Logf("Encountered %d errors during burst", totalErrors)
	}
}

// BenchmarkMemoryUsage tests memory efficiency
func BenchmarkMemoryUsage(b *testing.B) {
	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	boundary := "benchmark_test"

	// Pre-generate all events and stream IDs before timing
	events := make([]*grpcapi.EventToSave, b.N)
	streamIds := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		// Create large events to test memory handling
		largeData := make([]byte, 1024) // 1KB per event
		rand.Read(largeData)

		events[i] = &grpcapi.EventToSave{
			EventId:   uuid.New().String(),
			EventType: "LargeEvent",
			Data:      fmt.Sprintf(`{"large_field": "%s", "size": %d, "iteration": %d}`, base64.StdEncoding.EncodeToString(largeData), len(largeData), i),
			Metadata:  `{"source": "benchmark", "test": "memory"}`,
		}
		streamIds[i] = uuid.New().String()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := grpcapi.Position{CommitPosition: -1, PreparePosition: -1}
		_, err := setup.client.SaveEvents(setup.authContext(), &grpcapi.SaveEventsRequest{
			Boundary: boundary,
			Query: &grpcapi.SaveQuery{
				ExpectedPosition: &p,
			},
			Events: []*grpcapi.EventToSave{events[i]},
		})
		if err != nil {
			b.Errorf("Failed to save large event: %v", err)
		}
	}
}

// BenchmarkSaveEvents_DirectDatabase tests the PostgreSQL eventstore Save method directly
// bypassing gRPC to isolate database performance from network/gRPC overhead
func BenchmarkSaveEvents_DirectDatabase10K(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	setup := setupBenchmark(b)
	defer setup.cleanup(b)

	ctx := context.Background()

	// Create database connection
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/orisun?sslmode=disable", setup.postgresHost, setup.postgresPort)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Configure connection pool for high performance
	db.SetMaxOpenConns(250)
	db.SetMaxIdleConns(100)
	db.SetConnMaxLifetime(time.Hour)

	// Wait for database to be ready
	for i := range 30 {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(time.Second)
		if i == 29 {
			b.Fatalf("Database not ready after 30 seconds")
		}
	}

	// Run database migrations
	if err := postgres.RunDbScripts(db, "benchmark_test", "public", false, ctx); err != nil {
		b.Fatalf("Failed to migrate public schema: %v", err)
	}

	// Create PostgreSQL eventstore instance
	boundarySchemaMappings := map[string]config.BoundaryToPostgresSchemaMapping{
		"benchmark_test": {Schema: "public"},
	}

	logger, err := logging.ZapLogger("info")
	if err != nil {
		b.Fatalf("Failed to create logger: %v", err)
	}
	saveEvents := postgres.NewPostgresSaveEvents(ctx, db, logger, boundarySchemaMappings)

	// Prepare test events - 10K simultaneous saves to the SAME stream
	const totalEvents = 10000

	b.ReportAllocs()

	for b.Loop() {
		var wg sync.WaitGroup
		startSignal := make(chan struct{})
		var successfulSaves int64
		var totalAttempts int64

		// Pre-create ALL goroutines first, but don't let them execute yet
		for eventIndex := range totalEvents {
			wg.Add(1)

			// Create a single event with different criteria for each goroutine
			event := orisun.EventWithMapTags{
				EventId:   uuid.New().String(),
				EventType: "ConcurrentBenchmarkEvent",
				Data:      fmt.Sprintf(`{"event_id": %d, "criteria": "type_%d", "timestamp": "%s"}`, eventIndex, eventIndex%100, time.Now().Format(time.RFC3339)),
				Metadata:  fmt.Sprintf(`{"benchmark": "concurrent_same_stream", "event_index": %d}`, eventIndex),
			}

			// Create unique consistency condition for each event
			uniqueConsistencyCondition := &orisun.Query{
				Criteria: []*orisun.Criterion{
					{
						Tags: []*orisun.Tag{
							{
								Key:   "event_index",
								Value: fmt.Sprintf("%d", eventIndex),
							},
							{
								Key:   "benchmark_run",
								Value: fmt.Sprintf("%d", time.Now().UnixNano()),
							},
						},
					},
				},
			}

			go func(eventID int, event orisun.EventWithMapTags, consistencyCondition *orisun.Query) {
				defer wg.Done()

				// WAIT for the start signal - this ensures TRUE SIMULTANEITY!
				<-startSignal

				// ALL goroutines try to save to the SAME stream with expected version -1
				// This will test optimistic concurrency - only one should succeed, others should fail
				p := orisun.NotExistsPosition()
				prepared, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{event})
				if err != nil {
					b.Errorf("failed to prepare event: %v", err)
					return
				}
				_, _, err = saveEvents.SavePrepared(
					ctx,
					prepared,
					"benchmark_test",
					&p,                   // Expected position for a new stream.
					consistencyCondition, // Unique consistency condition for each event
				)

				// Use atomic operations to avoid mutex overhead
				atomic.AddInt64(&totalAttempts, 1)
				if err == nil {
					atomic.AddInt64(&successfulSaves, 1)
				}
				// Ignore concurrency errors - they're expected in this test
			}(eventIndex, event, uniqueConsistencyCondition)
		}

		// NOW start the timer and release all goroutines simultaneously
		start := time.Now()
		close(startSignal) // This releases ALL 10,000 goroutines at the EXACT same time!

		wg.Wait()
		duration := time.Since(start)

		finalTotalAttempts := atomic.LoadInt64(&totalAttempts)
		finalSuccessfulSaves := atomic.LoadInt64(&successfulSaves)
		attemptsPerSecond := float64(finalTotalAttempts) / duration.Seconds()

		// Focus on events per second metric
		b.ReportMetric(attemptsPerSecond, "events/sec")

		b.Logf("🚀 TRULY SIMULTANEOUS TEST: %d attempts (%d successful) in %.2f seconds at %.1f events/sec",
			finalTotalAttempts, finalSuccessfulSaves, duration.Seconds(), attemptsPerSecond)
	}
}

// BenchmarkSaveEvents_DirectDatabase10KBatch - 10K events in a single batch operation
func BenchmarkSaveEvents_DirectDatabase10KBatch(b *testing.B) {
	const totalEvents = 10000

	// Setup PostgreSQL container with optimized configuration
	req := testcontainers.ContainerRequest{
		Image:        "postgres:17.5-alpine3.22",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "orisun",
		},
		Cmd: []string{
			"postgres",
			// Memory settings
			"-c", "shared_buffers=2GB",
			"-c", "work_mem=64MB",
			"-c", "maintenance_work_mem=512MB",
			"-c", "effective_cache_size=6GB",
			// WAL settings for write performance
			"-c", "wal_buffers=64MB",
			"-c", "min_wal_size=2GB",
			"-c", "max_wal_size=8GB",
			"-c", "checkpoint_completion_target=0.7",
			"-c", "checkpoint_timeout=3min",
			// Connection and concurrency
			"-c", "max_connections=200",
			"-c", "max_worker_processes=14",
			"-c", "max_parallel_workers=14",
			"-c", "max_parallel_workers_per_gather=7",
			"-c", "max_parallel_maintenance_workers=7",
			// Performance tuning
			"-c", "random_page_cost=1.1",
			"-c", "seq_page_cost=1.0",
			"-c", "cpu_tuple_cost=0.01",
			"-c", "cpu_index_tuple_cost=0.005",
			"-c", "cpu_operator_cost=0.0025",
			// Logging for performance monitoring
			"-c", "log_min_duration_statement=1000",
			"-c", "log_checkpoints=on",
			"-c", "log_connections=on",
			"-c", "log_disconnections=on",
			"-c", "log_lock_waits=on",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}

	ctx := context.Background()
	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			b.Logf("Failed to terminate PostgreSQL container: %v", err)
		}
	}()

	// Get PostgreSQL connection details
	host, err := postgresContainer.Host(ctx)
	if err != nil {
		b.Fatalf("Failed to get PostgreSQL host: %v", err)
	}

	port, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		b.Fatalf("Failed to get PostgreSQL port: %v", err)
	}

	// Create database connection
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/orisun?sslmode=disable", host, port.Port())
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Configure connection pool for high performance
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Hour)

	// Wait for database to be ready
	for i := range 30 {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(time.Second)
		if i == 29 {
			b.Fatalf("Database not ready after 30 seconds")
		}
	}

	// Run database migrations
	if err := postgres.RunDbScripts(db, "benchmark_test", "public", false, ctx); err != nil {
		b.Fatalf("Failed to migrate public schema: %v", err)
	}

	// Create PostgreSQL eventstore instance
	boundarySchemaMappings := map[string]config.BoundaryToPostgresSchemaMapping{
		"benchmark_test": {Schema: "public"},
	}

	logger, err := logging.ZapLogger("info")
	if err != nil {
		b.Fatalf("Failed to create logger: %v", err)
	}
	saveEvents := postgres.NewPostgresSaveEvents(ctx, db, logger, boundarySchemaMappings)

	// Prepare test events - 10K events for batch save
	events := make([]orisun.EventWithMapTags, totalEvents)

	for i := range totalEvents {
		events[i] = orisun.EventWithMapTags{
			EventId:   uuid.New().String(),
			EventType: "BatchTestEvent",
			Data:      fmt.Sprintf(`{"message": "Batch event %d", "timestamp": "%s"}`, i, time.Now().Format(time.RFC3339)),
			Metadata:  fmt.Sprintf(`{"event_index": %d, "batch_test": true}`, i),
		}
	}
	prepared, err := orisun.PrepareEventsForSave(events)
	if err != nil {
		b.Fatalf("Failed to prepare batch events: %v", err)
	}

	// Reset timer to exclude setup time
	b.ResetTimer()
	b.ReportAllocs()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		start := time.Now()

		// Save all events in a single batch operation
		p := orisun.NotExistsPosition()
		_, _, err := saveEvents.SavePrepared(ctx, prepared, "benchmark_test", &p, nil)
		if err != nil {
			b.Fatalf("Failed to save batch events: %v", err)
		}

		duration := time.Since(start)
		eventsPerSecond := float64(totalEvents) / duration.Seconds()

		b.Logf("Batch save: %d events in %v (%.1f events/sec)",
			totalEvents, duration, eventsPerSecond)
	}
}
