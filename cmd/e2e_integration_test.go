package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	adminevents "github.com/OrisunLabs/Orisun/boundary/events"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/OrisunLabs/Orisun/orisun/grpcapi"
)

type E2ETestSuite struct {
	ctx               context.Context
	postgresContainer *postgres.PostgresContainer
	binaryPath        string
	binaryCmd         *exec.Cmd
	grpcConn          *grpc.ClientConn
	eventStoreClient  pb.EventStoreClient
	postgresHost      string
	postgresPort      string
	grpcPort          string
	adminPort         string
	natsPort          string
	natsStoreDir      string
	sqliteDir         string
	backend           string
	buildTags         string
	fdbClusterFile    string
	fdbRoot           string
}

func setupE2ETest(t *testing.T) *E2ETestSuite {
	suite := preparePostgresE2ETest(t)
	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)
	suite.createBoundary(t, "orisun_test_1", "postgres", "public")
	suite.createBoundary(t, "orisun_test_2", "postgres", "test2")
	return suite
}

func preparePostgresE2ETest(t *testing.T) *E2ETestSuite {
	ctx := context.Background()
	suite := &E2ETestSuite{
		ctx:       ctx,
		grpcPort:  "15005", // Use different port to avoid conflicts
		adminPort: "18991",
		natsPort:  "14224",
		backend:   "postgres",
	}

	// Start PostgreSQL container
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("orisun"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	suite.postgresContainer = postgresContainer

	// Get PostgreSQL connection details
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err)
	suite.postgresHost = host

	port, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	suite.postgresPort = port.Port()

	// Build the binary
	suite.buildBinary(t)

	return suite
}

func setupSQLiteE2ETest(t *testing.T) *E2ETestSuite {
	suite := prepareSQLiteE2ETest(t)
	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)
	suite.createBoundary(t, "orisun_test_1", "sqlite", "orisun_test_1")
	suite.createBoundary(t, "orisun_test_2", "sqlite", "orisun_test_2")
	return suite
}

func prepareSQLiteE2ETest(t *testing.T) *E2ETestSuite {
	ctx := context.Background()
	tempDir := t.TempDir()
	sqliteDir := filepath.Join(tempDir, "sqlite")
	require.NoError(t, os.MkdirAll(sqliteDir, 0o755))
	suite := &E2ETestSuite{
		ctx:          ctx,
		grpcPort:     "15007",
		adminPort:    "18993",
		natsPort:     "14226",
		natsStoreDir: filepath.Join(tempDir, "nats"),
		sqliteDir:    sqliteDir,
		backend:      "sqlite",
	}

	suite.buildBinary(t)

	return suite
}

func (s *E2ETestSuite) buildBinary(t *testing.T) {
	// Determine the target OS and architecture
	targetOS := runtime.GOOS
	targetArch := runtime.GOARCH

	// Create build directory if it doesn't exist
	buildDir := "./build"
	err := os.MkdirAll(buildDir, 0755)
	require.NoError(t, err)

	// Set binary name
	binaryName := fmt.Sprintf("orisun-%s-%s", targetOS, targetArch)
	if s.buildTags != "" {
		binaryName += "-" + s.buildTags
	}
	s.binaryPath = filepath.Join(buildDir, binaryName)

	tags := "development=false"
	if s.buildTags != "" {
		tags += "," + s.buildTags
	}

	// Build the binary using the same command as build.sh
	cmd := exec.Command("go", "build",
		"-tags", tags,
		"-a",
		"-installsuffix", "cgo",
		"-ldflags=-w -s",
		"-gcflags=-m",
		"-o", s.binaryPath,
		"./main.go")

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOOS=%s", targetOS),
		fmt.Sprintf("GOARCH=%s", targetArch),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build binary: %v\nOutput: %s", err, string(output))
	}

	t.Logf("Binary built successfully: %s", s.binaryPath)
}

func (s *E2ETestSuite) startBinary(t *testing.T) {
	// Set environment variables for the binary
	env := []string{
		fmt.Sprintf("ORISUN_BACKEND=%s", s.backend),
		fmt.Sprintf("ORISUN_PG_HOST=%s", s.postgresHost),
		fmt.Sprintf("ORISUN_PG_PORT=%s", s.postgresPort),
		"ORISUN_PG_USER=postgres",
		"ORISUN_PG_PASSWORD=postgres",
		"ORISUN_PG_NAME=orisun",
		"ORISUN_PG_ADMIN_SCHEMA=admin",
		fmt.Sprintf("ORISUN_GRPC_PORT=%s", s.grpcPort),
		fmt.Sprintf("ORISUN_ADMIN_PORT=%s", s.adminPort),
		"ORISUN_GRPC_ENABLE_REFLECTION=true",
		fmt.Sprintf("ORISUN_NATS_PORT=%s", s.natsPort),
		"ORISUN_NATS_CLUSTER_PORT=16222",
		"ORISUN_NATS_CLUSTER_ENABLED=false",
		"ORISUN_LOGGING_LEVEL=INFO",
		"ORISUN_ADMIN_USERNAME=admin",
		"ORISUN_ADMIN_PASSWORD=changeit",
		"ORISUN_ADMIN_BOUNDARY=orisun_admin",
	}
	if s.sqliteDir != "" {
		env = append(env, fmt.Sprintf("ORISUN_SQLITE_DIR=%s", s.sqliteDir))
	}
	if s.natsStoreDir != "" {
		env = append(env, fmt.Sprintf("ORISUN_NATS_STORE_DIR=%s", s.natsStoreDir))
	}
	if s.fdbClusterFile != "" {
		env = append(env, fmt.Sprintf("ORISUN_FDB_CLUSTER_FILE=%s", s.fdbClusterFile))
	}
	if s.fdbRoot != "" {
		env = append(env, fmt.Sprintf("ORISUN_FDB_ROOT=%s", s.fdbRoot))
	}

	env = append(os.Environ(), env...)

	// Start the binary
	s.binaryCmd = exec.Command(s.binaryPath)
	s.binaryCmd.Env = env
	s.binaryCmd.Stdout = os.Stdout
	s.binaryCmd.Stderr = os.Stderr

	err := s.binaryCmd.Start()
	require.NoError(t, err)

	t.Logf("Binary started with PID: %d", s.binaryCmd.Process.Pid)
}

func (s *E2ETestSuite) waitForGRPCServer(t *testing.T) {
	// Wait for the gRPC server to be ready
	maxRetries := 30
	for i := range maxRetries {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", s.grpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(1*time.Second),
		)
		if err == nil {
			conn.Close()
			t.Logf("gRPC server is ready after %d attempts", i+1)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("gRPC server did not start within expected time")
}

func (s *E2ETestSuite) createGRPCClient(t *testing.T) {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", s.grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	s.grpcConn = conn
	s.eventStoreClient = pb.NewEventStoreClient(conn)
}

func (s *E2ETestSuite) createBoundary(t *testing.T, name, backend, namespace string) {
	t.Helper()
	ctx := createAuthenticatedContext("admin", "changeit")
	adminClient := pb.NewAdminClient(s.grpcConn)
	_, err := adminClient.CreateBoundary(ctx, &pb.CreateBoundaryRequest{
		Name:      name,
		Placement: &pb.BoundaryPlacementInput{Backend: backend, Namespace: namespace},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, getErr := adminClient.GetBoundary(ctx, &pb.GetBoundaryRequest{Name: name})
		return getErr == nil &&
			response.Boundary != nil &&
			response.Boundary.Status == pb.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE
	}, 10*time.Second, 25*time.Millisecond)
}

// createAuthenticatedContext creates a context with Basic Auth headers
func createAuthenticatedContext(username, password string) context.Context {
	// Create Basic Auth header
	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))

	// Create metadata with the Authorization header
	md := metadata.New(map[string]string{
		"Authorization": authHeader,
	})

	// Attach metadata to the context
	return metadata.NewOutgoingContext(context.Background(), md)
}

func (s *E2ETestSuite) teardown(t *testing.T) {
	s.stopBinary(t)

	// Stop PostgreSQL container
	if s.postgresContainer != nil {
		err := s.postgresContainer.Terminate(s.ctx)
		if err != nil {
			t.Logf("Failed to terminate PostgreSQL container: %v", err)
		}
	}

	// Clean up binary
	if s.binaryPath != "" {
		os.Remove(s.binaryPath)
	}
}

func (s *E2ETestSuite) stopBinary(t *testing.T) {
	// Close gRPC connection
	if s.grpcConn != nil {
		if err := s.grpcConn.Close(); err != nil {
			t.Logf("Failed to close gRPC connection: %v", err)
		}
		s.grpcConn = nil
		s.eventStoreClient = nil
	}

	// Stop the binary
	if s.binaryCmd != nil && s.binaryCmd.Process != nil {
		signalErr := s.binaryCmd.Process.Signal(syscall.SIGTERM)
		if signalErr != nil {
			t.Logf("Failed to send SIGTERM to binary: %v", signalErr)
			// Force kill if SIGTERM fails
			if err := s.binaryCmd.Process.Kill(); err != nil {
				t.Logf("Failed to kill binary: %v", err)
			}
		}
		// Wait for process to exit
		if err := s.binaryCmd.Wait(); err != nil && signalErr == nil {
			t.Logf("Binary exited after SIGTERM: %v", err)
		}
		t.Logf("Binary process stopped")
		s.binaryCmd = nil
	}
}

func TestE2E_SaveAndGetEvents(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)

	// Create authenticated context with admin credentials
	ctx := createAuthenticatedContext("admin", "changeit")

	// Test SaveEvents
	position := pb.Position{CommitPosition: -1, PreparePosition: -1}
	saveReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &position,
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "TestEvent",
				Data:      `{"message": "Hello World"}`,
				Metadata:  `{"source": "e2e-test"}`,
			},
			{
				EventId:   uuid.New().String(),
				EventType: "TestEvent2",
				Data:      `{"message": "Hello World 2"}`,
				Metadata:  `{"source": "e2e-test"}`,
			},
		},
	}

	saveResp, err := suite.eventStoreClient.SaveEvents(ctx, saveReq)
	require.NoError(t, err)
	require.NotNil(t, saveResp)
	assert.GreaterOrEqual(t, saveResp.LogPosition.PreparePosition, int64(0))

	// Test GetEvents
	getReq := &pb.GetEventsRequest{
		Boundary:  "orisun_test_1",
		Count:     10,
		Direction: pb.Direction_ASC,
	}

	getResp, err := suite.eventStoreClient.GetEvents(ctx, getReq)
	require.NoError(t, err)
	require.NotNil(t, getResp)
	assert.Len(t, getResp.Events, 2)

	// Verify first event
	firstEvent := getResp.Events[0]
	assert.Equal(t, "TestEvent", firstEvent.EventType)
	assert.Contains(t, string(firstEvent.Data), "Hello World")

	// Verify second event
	secondEvent := getResp.Events[1]
	assert.Equal(t, "TestEvent2", secondEvent.EventType)
	assert.Contains(t, string(secondEvent.Data), "Hello World 2")
}

func TestE2E_SQLite_SaveAndGetEvents(t *testing.T) {
	suite := setupSQLiteE2ETest(t)
	defer suite.teardown(t)

	ctx := createAuthenticatedContext("admin", "changeit")
	position := pb.Position{CommitPosition: -1, PreparePosition: -1}
	saveReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &position,
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "SQLiteTestEvent",
				Data:      `{"message": "Hello SQLite"}`,
				Metadata:  `{"source": "sqlite-e2e-test"}`,
			},
		},
	}

	saveResp, err := suite.eventStoreClient.SaveEvents(ctx, saveReq)
	require.NoError(t, err)
	require.NotNil(t, saveResp.LogPosition)
	assert.GreaterOrEqual(t, saveResp.LogPosition.PreparePosition, int64(1))

	getResp, err := suite.eventStoreClient.GetEvents(ctx, &pb.GetEventsRequest{
		Boundary:  "orisun_test_1",
		Count:     10,
		Direction: pb.Direction_ASC,
		Query: &pb.Query{
			Criteria: []*pb.Criterion{
				{
					Tags: []*pb.Tag{
						{Key: "message", Value: "Hello SQLite"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Events, 1)
	assert.Equal(t, "SQLiteTestEvent", getResp.Events[0].EventType)
	assert.Contains(t, getResp.Events[0].Data, "Hello SQLite")
}

func TestE2E_Postgres_CatalogBoundarySurvivesRestart(t *testing.T) {
	const (
		boundary = "orisun_test_1"
		schema   = "public"
	)
	suite := preparePostgresE2ETest(t)
	defer suite.teardown(t)

	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)
	suite.createBoundary(t, boundary, "postgres", schema)

	ctx := createAuthenticatedContext("admin", "changeit")
	beforeRestart := requireActiveBoundary(t, suite, ctx, boundary, "postgres", schema, false)
	require.Equal(t, boundaryCatalogEventCounts{created: 1, activated: 1}, catalogEventCounts(t, suite, ctx, boundary))

	appendBoundaryEvent(t, suite, ctx, boundary, "OrderConfirmed", map[string]any{"orderId": "legacy-order-1"})
	requireBoundaryEventTypes(t, suite, ctx, boundary, "OrderConfirmed")

	suite.stopBinary(t)
	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)

	afterRestart := requireActiveBoundary(t, suite, ctx, boundary, "postgres", schema, false)
	requireSamePosition(t, beforeRestart.DefinitionPosition, afterRestart.DefinitionPosition)
	requireSamePosition(t, beforeRestart.StatusPosition, afterRestart.StatusPosition)
	require.Equal(t, boundaryCatalogEventCounts{created: 1, activated: 1}, catalogEventCounts(t, suite, ctx, boundary))
	requireBoundaryEventTypes(t, suite, ctx, boundary, "OrderConfirmed")
}

func TestE2E_SQLite_CatalogBoundarySurvivesRestart(t *testing.T) {
	const boundary = "orisun_test_1"
	suite := prepareSQLiteE2ETest(t)
	defer suite.teardown(t)

	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)
	suite.createBoundary(t, boundary, "sqlite", boundary)

	ctx := createAuthenticatedContext("admin", "changeit")
	beforeRestart := requireActiveBoundary(t, suite, ctx, boundary, "sqlite", boundary, false)
	require.Equal(t, boundaryCatalogEventCounts{created: 1, activated: 1}, catalogEventCounts(t, suite, ctx, boundary))

	appendBoundaryEvent(t, suite, ctx, boundary, "OrderConfirmed", map[string]any{"orderId": "legacy-order-1"})
	requireBoundaryEventTypes(t, suite, ctx, boundary, "OrderConfirmed")

	suite.stopBinary(t)
	suite.startBinary(t)
	suite.waitForGRPCServer(t)
	suite.createGRPCClient(t)

	afterRestart := requireActiveBoundary(t, suite, ctx, boundary, "sqlite", boundary, false)
	requireSamePosition(t, beforeRestart.DefinitionPosition, afterRestart.DefinitionPosition)
	requireSamePosition(t, beforeRestart.StatusPosition, afterRestart.StatusPosition)
	require.Equal(t, boundaryCatalogEventCounts{created: 1, activated: 1}, catalogEventCounts(t, suite, ctx, boundary))
	requireBoundaryEventTypes(t, suite, ctx, boundary, "OrderConfirmed")
}

type boundaryCatalogEventCounts struct {
	created   int
	activated int
}

func requireActiveBoundary(
	t *testing.T,
	suite *E2ETestSuite,
	ctx context.Context,
	boundary,
	backend,
	namespace string,
	existedBeforeCatalog bool,
) *pb.BoundaryInfo {
	t.Helper()
	adminClient := pb.NewAdminClient(suite.grpcConn)
	var info *pb.BoundaryInfo
	require.Eventually(t, func() bool {
		response, err := adminClient.GetBoundary(ctx, &pb.GetBoundaryRequest{Name: boundary})
		if err != nil || response.Boundary == nil {
			return false
		}
		info = response.Boundary
		return info.Status == pb.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE
	}, 10*time.Second, 25*time.Millisecond)
	require.Equal(t, boundary, info.Name)
	require.Equal(t, existedBeforeCatalog, info.ExistedBeforeCatalog)
	require.Equal(t, backend, info.Placement.Backend)
	require.Equal(t, namespace, info.Placement.Namespace)
	require.NotNil(t, info.DefinitionPosition)
	require.NotNil(t, info.StatusPosition)
	return info
}

func catalogEventCounts(
	t *testing.T,
	suite *E2ETestSuite,
	ctx context.Context,
	boundary string,
) boundaryCatalogEventCounts {
	t.Helper()
	response, err := suite.eventStoreClient.GetEvents(ctx, &pb.GetEventsRequest{
		Boundary:  "orisun_admin",
		Count:     100,
		Direction: pb.Direction_ASC,
	})
	require.NoError(t, err)
	counts := boundaryCatalogEventCounts{}
	for _, event := range response.Events {
		var data struct {
			Boundary string `json:"boundary"`
		}
		require.NoError(t, json.Unmarshal([]byte(event.Data), &data))
		if data.Boundary != boundary {
			continue
		}
		switch event.EventType {
		case adminevents.EventTypeBoundaryCreated:
			counts.created++
		case adminevents.EventTypeBoundaryActivated:
			counts.activated++
		}
	}
	return counts
}

func appendBoundaryEvent(
	t *testing.T,
	suite *E2ETestSuite,
	ctx context.Context,
	boundary,
	eventType string,
	data map[string]any,
) {
	t.Helper()
	dataJSON, err := json.Marshal(data)
	require.NoError(t, err)
	_, err = suite.eventStoreClient.SaveEvents(ctx, &pb.SaveEventsRequest{
		Boundary: boundary,
		Events: []*pb.EventToSave{{
			EventId:   uuid.NewString(),
			EventType: eventType,
			Data:      string(dataJSON),
			Metadata:  `{"source":"catalog-migration-e2e"}`,
		}},
	})
	require.NoError(t, err)
}

func requireBoundaryEventTypes(
	t *testing.T,
	suite *E2ETestSuite,
	ctx context.Context,
	boundary string,
	expected ...string,
) {
	t.Helper()
	response, err := suite.eventStoreClient.GetEvents(ctx, &pb.GetEventsRequest{
		Boundary:  boundary,
		Count:     100,
		Direction: pb.Direction_ASC,
	})
	require.NoError(t, err)
	actual := make([]string, len(response.Events))
	for i, event := range response.Events {
		actual[i] = event.EventType
	}
	require.Equal(t, expected, actual)
}

func requireSamePosition(t *testing.T, before, after *pb.Position) {
	t.Helper()
	require.NotNil(t, before)
	require.NotNil(t, after)
	require.Equal(t, before.CommitPosition, after.CommitPosition)
	require.Equal(t, before.PreparePosition, after.PreparePosition)
}

func TestE2E_SQLite_CreateBoundary(t *testing.T) {
	suite := setupSQLiteE2ETest(t)
	defer suite.teardown(t)

	ctx := createAuthenticatedContext("admin", "changeit")
	adminClient := pb.NewAdminClient(suite.grpcConn)
	created, err := adminClient.CreateBoundary(ctx, &pb.CreateBoundaryRequest{
		Name:        "dynamic_sales",
		Description: "Dynamic SQLite boundary",
		Placement:   &pb.BoundaryPlacementInput{Backend: "sqlite", Namespace: "dynamic_sales"},
	})
	require.NoError(t, err)
	require.Equal(t, pb.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_PROVISIONING, created.Boundary.Status)

	require.Eventually(t, func() bool {
		response, getErr := adminClient.GetBoundary(ctx, &pb.GetBoundaryRequest{Name: "dynamic_sales"})
		return getErr == nil && response.Boundary.Status == pb.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE
	}, 5*time.Second, 25*time.Millisecond)

	_, err = suite.eventStoreClient.SaveEvents(ctx, &pb.SaveEventsRequest{
		Boundary: "dynamic_sales",
		Events: []*pb.EventToSave{{
			EventId: uuid.NewString(), EventType: "SaleOpened", Data: `{"sale_id":"1"}`, Metadata: `{}`,
		}},
	})
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(suite.sqliteDir, "dynamic_sales.db"))
}

func TestE2E_OptimisticConcurrency(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)

	// Create authenticated context with admin credentials
	ctx := createAuthenticatedContext("admin", "changeit")

	// Save first event
	expectedPosition := pb.Position{CommitPosition: -1, PreparePosition: -1}
	firstSaveReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &expectedPosition,
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "FirstEvent",
				Data:      `{"message": "First"}`,
				Metadata:  `{"source": "e2e-test"}`,
			},
		},
	}

	firstSaveResp, err := suite.eventStoreClient.SaveEvents(ctx, firstSaveReq)
	require.NoError(t, err)
	require.NotNil(t, firstSaveResp.LogPosition)

	// Try to save with wrong expected version (should fail)
	wrongVersionReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &expectedPosition,
			SubsetQuery: &pb.Query{
				Criteria: []*pb.Criterion{
					{
						Tags: []*pb.Tag{
							{Key: "eventType", Value: "FirstEvent"},
						},
					},
				},
			},
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "SecondEvent",
				Data:      `{"message": "Second"}`,
				Metadata:  `{"source": "e2e-test"}`,
			},
		},
	}

	_, err = suite.eventStoreClient.SaveEvents(ctx, wrongVersionReq)
	assert.Error(t, err, "Expected optimistic concurrency error")

	// Save with correct expected version (should succeed)
	correctVersionReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: firstSaveResp.LogPosition, // Correct version
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "SecondEvent",
				Data:      `{"message": "Second"}`,
				Metadata:  `{"source": "e2e-test"}`,
			},
		},
	}

	correctSaveResp, err := suite.eventStoreClient.SaveEvents(ctx, correctVersionReq)
	require.NoError(t, err)
	require.NotNil(t, correctSaveResp.LogPosition)
}

func TestE2E_MultipleBoundaries(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)

	// Create authenticated context with admin credentials
	ctx := createAuthenticatedContext("admin", "changeit")

	// Save events in different boundaries
	boundaries := []string{"orisun_test_1", "orisun_test_2"}

	expectedPosition := pb.Position{CommitPosition: -1, PreparePosition: -1}
	for i, boundary := range boundaries {
		saveReq := &pb.SaveEventsRequest{
			Boundary: boundary,
			Query: &pb.SaveQuery{
				ExpectedPosition: &expectedPosition,
			},
			Events: []*pb.EventToSave{
				{
					EventId:   uuid.New().String(),
					EventType: "BoundaryEvent",
					Data:      fmt.Sprintf(`{"boundary": "%s", "index": %d}`, boundary, i),
					Metadata:  `{"source": "e2e-test"}`,
				},
			},
		}

		saveResp, err := suite.eventStoreClient.SaveEvents(ctx, saveReq)
		require.NoError(t, err)
		require.NotNil(t, saveResp.LogPosition)
	}

	// Get events from each boundary
	for _, boundary := range boundaries {
		getReq := &pb.GetEventsRequest{
			Boundary:  boundary,
			Count:     10,
			Direction: pb.Direction_ASC,
		}

		getResp, err := suite.eventStoreClient.GetEvents(ctx, getReq)
		require.NoError(t, err)
		assert.Len(t, getResp.Events, 1)
		assert.Equal(t, "BoundaryEvent", getResp.Events[0].EventType)
		assert.Contains(t, string(getResp.Events[0].Data), boundary)
	}
}

func TestE2E_CatchUpSubscribeToEvents(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)

	// Create authenticated context with admin credentials
	ctx, cancel := context.WithTimeout(createAuthenticatedContext("admin", "changeit"), 30*time.Second)
	defer cancel()

	// Save events first
	expectedPosition := pb.Position{CommitPosition: -1, PreparePosition: -1}
	saveReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &expectedPosition,
		},
		Events: []*pb.EventToSave{
			{
				EventId:   uuid.New().String(),
				EventType: "SubscriptionTest",
				Data:      `{"message": "First subscription event"}`,
				Metadata:  `{"source": "e2e-subscription-test"}`,
			},
		},
	}

	_, err := suite.eventStoreClient.SaveEvents(ctx, saveReq)
	require.NoError(t, err)

	// Create subscription request to catch up from beginning
	subscribeReq := &pb.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "orisun_test_1",
		SubscriberName: "test-subscriber",
		AfterPosition:  &pb.Position{CommitPosition: 0, PreparePosition: 0}, // Start from beginning
		Query: &pb.Query{
			Criteria: []*pb.Criterion{
				{
					Tags: []*pb.Tag{
						{Key: "eventType", Value: "SubscriptionTest"}, // Note: eventType, not event_type
					},
				},
			},
		},
	}

	// Start subscription
	stream, err := suite.eventStoreClient.CatchUpSubscribeToEvents(ctx, subscribeReq)
	require.NoError(t, err)

	// Try to receive at least one event
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive event from subscription: %v", err)
	}

	// Verify the received event
	assert.Equal(t, "SubscriptionTest", event.EventType)
	assert.Contains(t, event.Data, "First subscription event")
	t.Logf("Successfully received event: %s", event.EventId)
}

func TestE2E_PostgresLiveSubscribeToEvents(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)

	ctx, cancel := context.WithTimeout(createAuthenticatedContext("admin", "changeit"), 45*time.Second)
	defer cancel()

	eventType := "LiveSubscriptionTest"
	eventID := uuid.New().String()
	subscribeReq := &pb.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "orisun_test_1",
		SubscriberName: "live-test-subscriber",
		Query: &pb.Query{
			Criteria: []*pb.Criterion{
				{
					Tags: []*pb.Tag{
						{Key: "eventType", Value: eventType},
					},
				},
			},
		},
	}

	stream, err := suite.eventStoreClient.CatchUpSubscribeToEvents(ctx, subscribeReq)
	require.NoError(t, err)
	defer stream.CloseSend()

	receivedEvents := make(chan *pb.Event, 1)
	receiveErrors := make(chan error, 1)
	go func() {
		event, err := stream.Recv()
		if err != nil {
			receiveErrors <- err
			return
		}
		receivedEvents <- event
	}()

	// Give the server time to complete the empty catch-up phase and attach the
	// live NATS consumer before writing. This test is specifically guarding the
	// write-after-subscribe path for the Postgres backend.
	time.Sleep(2 * time.Second)

	expectedPosition := pb.Position{CommitPosition: -1, PreparePosition: -1}
	saveReq := &pb.SaveEventsRequest{
		Boundary: "orisun_test_1",
		Query: &pb.SaveQuery{
			ExpectedPosition: &expectedPosition,
		},
		Events: []*pb.EventToSave{
			{
				EventId:   eventID,
				EventType: eventType,
				Data:      `{"message": "Live subscription event"}`,
				Metadata:  `{"source": "e2e-live-subscription-test"}`,
			},
		},
	}

	_, err = suite.eventStoreClient.SaveEvents(ctx, saveReq)
	require.NoError(t, err)

	select {
	case event := <-receivedEvents:
		assert.Equal(t, eventID, event.EventId)
		assert.Equal(t, eventType, event.EventType)
		assert.Contains(t, event.Data, "Live subscription event")
	case err := <-receiveErrors:
		t.Fatalf("Failed to receive live event from subscription: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("Timed out waiting for live subscription event")
	}
}
