package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	logging "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// PostgreSQL driver
	_ "github.com/lib/pq"
)

type PostgresContainer struct {
	container testcontainers.Container
	host      string
	port      string
}

func requirePosition(t *testing.T, e orisun.ReadEvent, commit, prepare int64) {
	t.Helper()
	require.Equal(t, commit, e.CommitPosition)
	require.Equal(t, prepare, e.PreparePosition)
}

type YugabyteContainer struct {
	container testcontainers.Container
	host      string
	port      string
}

func setupTestContainer(t *testing.T) (*PostgresContainer, error) {
	ctx := context.Background() // Use background context instead of t.Context()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections"),
			wait.ForListeningPort("5432/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %v", err)
	}

	// Add a small delay to ensure the container is fully ready
	time.Sleep(2 * time.Second)

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, fmt.Errorf("failed to get container port: %v", err)
	}

	return &PostgresContainer{
		container: container,
		host:      host,
		port:      port.Port(),
	}, nil
}

func setupYugabyteTestContainer(t *testing.T) (*YugabyteContainer, error) {
	ctx := context.Background()
	image := os.Getenv("ORISUN_YUGABYTE_TEST_IMAGE")
	if image == "" {
		image = "yugabytedb/yugabyte:2025.2.3.2-b1"
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5433/tcp", "15433/tcp"},
		Cmd: []string{
			"bin/yugabyted",
			"start",
			"--background=false",
			"--tserver_flags=ysql_yb_enable_listen_notify=true",
			"--master_flags=ysql_yb_enable_listen_notify=true",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5433/tcp"),
			wait.ForLog("YugabyteDB Started").WithStartupTimeout(6*time.Minute),
		).WithStartupTimeout(6 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start yugabyte container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get yugabyte container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "5433/tcp")
	if err != nil {
		return nil, fmt.Errorf("failed to get yugabyte mapped ysql port: %w", err)
	}

	return &YugabyteContainer{
		container: container,
		host:      host,
		port:      port.Port(),
	}, nil
}

func setupTestDatabase(t *testing.T, container *PostgresContainer) (*sql.DB, error) {
	return setupTestDatabaseWithDialect(t, container, "postgres")
}

func setupTestDatabaseWithDialect(t *testing.T, container *PostgresContainer, dialect string) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=test password=test dbname=testdb sslmode=disable",
		container.host,
		container.port,
	)

	// Add connection timeout and retry logic
	var db *sql.DB
	var err error

	for retries := range 3 {
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to database: %v", err)
		}

		// Set connection parameters
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(time.Minute * 5)

		// Try to ping with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(ctx)
		cancel()

		if err == nil {
			break
		}

		db.Close()
		time.Sleep(time.Duration(retries+1) * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to ping database after retries: %v", err)
	}

	// Run database migrations using the common scripts
	ctx := context.Background()
	if err := RunDbScriptsWithDialect(db, "test_boundary", "public", false, dialect, ctx); err != nil {
		return nil, fmt.Errorf("failed to run database migrations: %v", err)
	}

	return db, nil
}

func setupYugabyteTestDatabase(t *testing.T, container *YugabyteContainer) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=yugabyte password=yugabyte dbname=yugabyte sslmode=disable",
		container.host,
		container.port,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to yugabyte: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	var pingErr error
	for attempt := range 60 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr = db.PingContext(ctx)
		cancel()
		if pingErr == nil {
			break
		}
		time.Sleep(time.Duration(attempt/10+1) * time.Second)
	}
	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping yugabyte after retries: %w", pingErr)
	}

	if err := RunDbScriptsWithDialect(db, "test_boundary", "public", false, "yugabyte", context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run yugabyte migrations: %w", err)
	}

	return db, nil
}

func terminateTestContainer(t *testing.T, container testcontainers.Container) {
	t.Helper()
	if err := container.Terminate(context.Background()); err != nil {
		t.Logf("Failed to terminate container: %v", err)
	}
}

func TestSaveAndGetEvents(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	eventId, err := uuid.NewV7()
	require.NoError(t, err)

	// Test saving events
	events := []orisun.EventWithMapTags{
		{
			EventId:   eventId.String(),
			EventType: "TestEvent",
			Data:      "{\"key\": \"value\"}",
			Metadata:  "{\"meta\": \"data\", \"tags\": [{\"key\": \"key\", \"value\": \"value\"}]}",
		},
	}

	position := orisun.NotExistsPosition()
	// Save events
	tranID, globalID, err := saveEvents.Save(
		t.Context(),
		events,
		"test_boundary",
		&position,
		nil,
	)

	assert.NoError(t, err)
	assert.NotEmpty(t, tranID)
	assert.GreaterOrEqual(t, globalID, int64(0))
	assert.Equal(t, "1", tranID)
	assert.Equal(t, int64(0), globalID)

	// Get events
	resp, err := getEvents.GetBatch(
		t.Context(),
		&orisun.GetEventsRequest{
			Boundary:  "test_boundary",
			Direction: orisun.Direction_ASC,
			Count:     10,
		},
	)

	assert.NoError(t, err)
	assert.Len(t, resp, 1)
	assert.Equal(t, eventId.String(), resp[0].EventId)
	assert.Equal(t, "TestEvent", resp[0].EventType)
	requirePosition(t, resp[0], 1, 0)

	// Check that the data contains the expected values
	assert.Contains(t, resp[0].Data, "key")
	assert.Contains(t, resp[0].Data, "value")

	// Check that the metadata contains the expected values
	assert.Contains(t, resp[0].Metadata, "meta")
	assert.Contains(t, resp[0].Metadata, "data")
	assert.Contains(t, resp[0].Metadata, "tags")

	fromBeginning, err := getEvents.GetBatch(
		t.Context(),
		&orisun.GetEventsRequest{
			Boundary:     "test_boundary",
			Direction:    orisun.Direction_ASC,
			Count:        10,
			FromPosition: &orisun.Position{CommitPosition: 0, PreparePosition: 0},
		},
	)
	assert.NoError(t, err)
	assert.Len(t, fromBeginning, 1)
	assert.Equal(t, eventId.String(), fromBeginning[0].EventId)
}

func TestRunDbScripts_MaintainsCurrentPostgresStorage(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		INSERT INTO public.test_boundary_orisun_es_event
			(transaction_id, global_id, event_id, data, metadata)
		VALUES
			(2, 0, $1, '{"eventType":"CurrentEvent","key":"first"}', '{}'),
			(2, 1, $2, '{"eventType":"CurrentEvent","key":"second"}', '{}'),
			(3, 2, $3, '{"eventType":"CurrentEvent","key":"third"}', '{}')
	`, uuid.NewString(), uuid.NewString(), uuid.NewString())
	require.NoError(t, err)

	_, err = db.Exec(`SELECT setval('public.test_boundary_orisun_es_event_global_id_seq', 2, true)`)
	require.NoError(t, err)

	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, context.Background()))

	rows, err := db.Query(`
		SELECT transaction_id, global_id
		FROM public.test_boundary_orisun_es_event
		ORDER BY global_id ASC
	`)
	require.NoError(t, err)
	defer rows.Close()

	var positions [][2]int64
	for rows.Next() {
		var transactionID, globalID int64
		require.NoError(t, rows.Scan(&transactionID, &globalID))
		positions = append(positions, [2]int64{transactionID, globalID})
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [][2]int64{{2, 0}, {2, 1}, {3, 2}}, positions)

	var visibilityIndexDef string
	err = db.QueryRow(`
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'public'
		  AND tablename = 'test_boundary_orisun_es_event'
		  AND indexname = 'test_boundary_idx_event_order_visibility_covering'
	`).Scan(&visibilityIndexDef)
	require.NoError(t, err)
	require.Contains(t, visibilityIndexDef, "pg_xact_id")
	require.NotContains(t, visibilityIndexDef, "event_type")

	var eventTypeIndexDef string
	err = db.QueryRow(`
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'public'
		  AND tablename = 'test_boundary_orisun_es_event'
		  AND indexname = 'test_boundary_idx_event_type_order'
	`).Scan(&eventTypeIndexDef)
	require.NoError(t, err)
	require.Contains(t, eventTypeIndexDef, "data ->> 'eventType'::text")

	var eventTypeColumnCount int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'test_boundary_orisun_es_event'
		  AND column_name = 'event_type'
	`).Scan(&eventTypeColumnCount)
	require.NoError(t, err)
	require.Equal(t, 0, eventTypeColumnCount)

	var rowsMissingEventType int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM public.test_boundary_orisun_es_event
		WHERE data->>'eventType' IS DISTINCT FROM 'CurrentEvent'
	`).Scan(&rowsMissingEventType)
	require.NoError(t, err)
	require.Equal(t, 0, rowsMissingEventType)

	_, err = db.Exec(`
		UPDATE public.test_boundary_orisun_es_event
		SET pg_xact_id = 900000
	`)
	require.NoError(t, err)

	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, context.Background()))

	var stalePGXactIDCount int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM public.test_boundary_orisun_es_event
		WHERE pg_xact_id IS NOT NULL
	`).Scan(&stalePGXactIDCount)
	require.NoError(t, err)
	require.Equal(t, 0, stalePGXactIDCount)

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)
	expectedPosition := &orisun.Position{CommitPosition: 3, PreparePosition: 2}
	transactionID, globalID, err := saveEvents.Save(
		t.Context(),
		[]orisun.EventWithMapTags{
			{
				EventId:   uuid.NewString(),
				EventType: "PostUpgradeEvent",
				Data:      `{"key":"after-upgrade"}`,
				Metadata:  `{}`,
			},
		},
		"test_boundary",
		expectedPosition,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(3), globalID)
	require.Equal(t, "4", transactionID)

	resp, err := getEvents.GetBatch(t.Context(), &orisun.GetEventsRequest{
		Boundary:     "test_boundary",
		Direction:    orisun.Direction_ASC,
		Count:        10,
		FromPosition: &orisun.Position{CommitPosition: 0, PreparePosition: 0},
	})
	require.NoError(t, err)
	require.Len(t, resp, 4)
	requirePosition(t, resp[0], 2, 0)
	requirePosition(t, resp[1], 2, 1)
	requirePosition(t, resp[2], 3, 2)
	requirePosition(t, resp[3], 4, 3)

	var pgXactID sql.NullInt64
	err = db.QueryRow(`
		SELECT pg_xact_id
		FROM public.test_boundary_orisun_es_event
		WHERE global_id = 3
	`).Scan(&pgXactID)
	require.NoError(t, err)
	require.True(t, pgXactID.Valid)
}

func TestYugabyteDialectUsesCommittedWatermark(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabaseWithDialect(t, container, "yugabyte")
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	position := orisun.NotExistsPosition()
	transactionID, globalID, err := saveEvents.Save(
		t.Context(),
		[]orisun.EventWithMapTags{
			{
				EventId:   uuid.NewString(),
				EventType: "YugabyteWatermarkEvent",
				Data:      `{"key":"value"}`,
				Metadata:  `{}`,
			},
		},
		"test_boundary",
		&position,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, "1", transactionID)
	require.Equal(t, int64(0), globalID)

	var watermarkTransactionID, watermarkGlobalID int64
	err = db.QueryRow(`
		SELECT transaction_id, global_id
		FROM public.test_boundary_orisun_committed_position
		WHERE boundary = 'test_boundary'
	`).Scan(&watermarkTransactionID, &watermarkGlobalID)
	require.NoError(t, err)
	require.Equal(t, int64(1), watermarkTransactionID)
	require.Equal(t, int64(0), watermarkGlobalID)

	var pgXactID sql.NullInt64
	err = db.QueryRow(`
		SELECT pg_xact_id
		FROM public.test_boundary_orisun_es_event
		WHERE global_id = 0
	`).Scan(&pgXactID)
	require.NoError(t, err)
	require.False(t, pgXactID.Valid)

	_, err = db.Exec(`
		UPDATE public.test_boundary_orisun_committed_position
		SET transaction_id = -1, global_id = -1
		WHERE boundary = 'test_boundary'
	`)
	require.NoError(t, err)

	hidden, err := getEvents.GetBatch(t.Context(), &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
	})
	require.NoError(t, err)
	require.Empty(t, hidden)

	_, err = db.Exec(`
		UPDATE public.test_boundary_orisun_committed_position
		SET transaction_id = 1, global_id = 0
		WHERE boundary = 'test_boundary'
	`)
	require.NoError(t, err)

	visible, err := getEvents.GetBatch(t.Context(), &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
	})
	require.NoError(t, err)
	require.Len(t, visible, 1)
	requirePosition(t, visible[0], 1, 0)
}

func newYugabyteNotifyListenerWithRetry(
	ctx context.Context,
	connStr string,
	mapping map[string]config.BoundaryToPostgresSchemaMapping,
	logger logging.Logger,
) (*PGNotifyListener, error) {
	for {
		listener, err := NewPGNotifyListener(ctx, connStr, mapping, logger)
		if err == nil {
			return listener, nil
		}

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) ||
			pgErr.Code != "55000" ||
			!strings.Contains(pgErr.Message, "creating internal objects for LISTEN/NOTIFY") {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("Yugabyte LISTEN/NOTIFY did not become ready: %w", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func TestYugabyteContainerDialectSaveGetAndNotify(t *testing.T) {
	container, err := setupYugabyteTestContainer(t)
	require.NoError(t, err)
	defer terminateTestContainer(t, container.container)

	db, err := setupYugabyteTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=yugabyte password=yugabyte dbname=yugabyte sslmode=disable",
		container.host,
		container.port,
	)
	listenerInitCtx, cancelListenerInit := context.WithTimeout(t.Context(), 30*time.Second)
	listener, err := newYugabyteNotifyListenerWithRetry(listenerInitCtx, connStr, mapping, logger)
	cancelListenerInit()
	require.NoError(t, err)
	listenerCtx, cancelListener := context.WithCancel(t.Context())
	go listener.Start(listenerCtx)
	defer func() {
		cancelListener()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		listener.Close(closeCtx)
	}()

	signal := listener.Signal("test_boundary", time.Hour)
	defer signal.Stop()

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	eventID := uuid.NewString()
	position := orisun.NotExistsPosition()
	transactionID, globalID, err := saveEvents.Save(
		t.Context(),
		[]orisun.EventWithMapTags{
			{
				EventId:   eventID,
				EventType: "YugabyteContainerEvent",
				Data:      `{"account_id":"acct-yb","amount":"10"}`,
				Metadata:  `{}`,
			},
		},
		"test_boundary",
		&position,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, "1", transactionID)
	require.Equal(t, int64(0), globalID)

	waitCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, signal.Wait(waitCtx), "Yugabyte LISTEN/NOTIFY should wake the boundary listener")

	resp, err := getEvents.GetBatch(t.Context(), &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
	})
	require.NoError(t, err)
	require.Len(t, resp, 1)
	require.Equal(t, eventID, resp[0].EventId)
	requirePosition(t, resp[0], 1, 0)

	var watermarkTransactionID, watermarkGlobalID int64
	err = db.QueryRow(`
		SELECT transaction_id, global_id
		FROM public.test_boundary_orisun_committed_position
		WHERE boundary = 'test_boundary'
	`).Scan(&watermarkTransactionID, &watermarkGlobalID)
	require.NoError(t, err)
	require.Equal(t, int64(1), watermarkTransactionID)
	require.Equal(t, int64(0), watermarkGlobalID)

	var pgXactID sql.NullInt64
	err = db.QueryRow(`
		SELECT pg_xact_id
		FROM public.test_boundary_orisun_es_event
		WHERE global_id = 0
	`).Scan(&pgXactID)
	require.NoError(t, err)
	require.False(t, pgXactID.Valid)
}

func TestInsertsSerializePerBoundaryAndPositionsFollowCommitOrder(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	// The per-boundary position lock makes the old hazard structurally
	// impossible: a younger insert cannot draw positions (let alone commit)
	// while an older insert in the same boundary is still open. This test
	// asserts the serialisation and the resulting commit-ordered positions —
	// the property the publisher and the CCC max-position check rely on.
	ctx := t.Context()
	txOlder, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer txOlder.Rollback()

	olderID := uuid.NewString()
	olderGid, err := insertEventInTx(ctx, txOlder, olderID, "OlderOpenTransaction")
	require.NoError(t, err)

	type youngerResult struct {
		gid int64
		err error
	}
	youngerID := uuid.NewString()
	youngerDone := make(chan youngerResult, 1)
	go func() {
		txYounger, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			youngerDone <- youngerResult{err: err}
			return
		}
		defer txYounger.Rollback()
		gid, err := insertEventInTx(context.Background(), txYounger, youngerID, "YoungerTransaction")
		if err == nil {
			err = txYounger.Commit()
		}
		youngerDone <- youngerResult{gid: gid, err: err}
	}()

	select {
	case res := <-youngerDone:
		t.Fatalf("younger insert must block on the per-boundary position lock while the older insert is open, got %+v", res)
	case <-time.After(750 * time.Millisecond):
	}

	resp, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
	})
	require.NoError(t, err)
	assert.Empty(t, resp, "nothing must be visible while the only insert is still open")

	require.NoError(t, txOlder.Commit())

	var younger youngerResult
	select {
	case younger = <-youngerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("younger insert did not complete after the older transaction committed")
	}
	require.NoError(t, younger.err)
	require.Greater(t, younger.gid, olderGid, "positions must be assigned in commit order")

	require.Eventually(t, func() bool {
		resp, err = getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
			Boundary:  "test_boundary",
			Direction: orisun.Direction_ASC,
			Count:     10,
		})
		return err == nil && len(resp) == 2
	}, 5*time.Second, 50*time.Millisecond)

	require.Equal(t, olderID, resp[0].EventId)
	require.Equal(t, youngerID, resp[1].EventId)
}

func insertEventInTx(ctx context.Context, tx *sql.Tx, eventID, eventType string) (int64, error) {
	eventsJSON := fmt.Sprintf(
		`[{"event_id":%q,"event_type":%q,"data":{"eventType":%q},"metadata":{}}]`,
		eventID,
		eventType,
		eventType,
	)

	var newGlobalID, latestTransactionID, latestGlobalID int64
	err := tx.QueryRowContext(
		ctx,
		`SELECT * FROM public.insert_events_with_consistency_v3($1::text, $2::text, $3::jsonb, $4::jsonb)`,
		"test_boundary",
		"public",
		[]byte(`{}`),
		[]byte(eventsJSON),
	).Scan(&newGlobalID, &latestTransactionID, &latestGlobalID)
	return latestGlobalID, err
}

func TestSave200EventsOneByOne(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	expectedPosition := orisun.NotExistsPosition()

	// Save 100 events one by one
	for i := range 200 {
		eventId, err := uuid.NewV7()
		require.NoError(t, err)

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "SequentialEvent",
				Data:      fmt.Sprintf("{\"sequence\": %d, \"message\": \"Event number %d\"}", i, i),
				Metadata:  fmt.Sprintf("{\"event_number\": %d, \"timestamp\": \"%s\"}", i, time.Now().Format(time.RFC3339)),
			},
		}

		// Save the event
		tranID, globalID, err := saveEvents.Save(
			t.Context(),
			events,
			"test_boundary",
			&expectedPosition,
			nil,
		)

		require.NoError(t, err, "Failed to save event %d", i)
		assert.NotEmpty(t, tranID, "Transaction ID should not be empty for event %d", i)
		assert.GreaterOrEqual(t, globalID, int64(0), "Global ID should be greater than or equal to 0 for event %d", i)

		transactionIDInt, err := strconv.ParseInt(tranID, 10, 64)
		require.NoError(t, err)
		expectedPosition = orisun.Position{
			PreparePosition: globalID,
			CommitPosition:  transactionIDInt,
		}
	}

	// Verify all 100 events were saved correctly
	resp, err := getEvents.GetBatch(
		t.Context(),
		&orisun.GetEventsRequest{
			Boundary:  "test_boundary",
			Direction: orisun.Direction_ASC,
			Count:     100,
		},
	)

	require.NoError(t, err)
	assert.Len(t, resp, 100, "Should have exactly 100 events")

	// // Verify the sequence and content of events
	// for i, event := range resp {
	// 	assert.Equal(t, "SequentialEvent", event.EventType, "Event %d should have correct type", i)
	// 	assert.Contains(t, event.Data, fmt.Sprintf("\"sequence\": %d", i), "Event %d should have correct sequence in data", i)
	// 	assert.Contains(t, event.Data, fmt.Sprintf("\"message\": \"Event number %d\"", i), "Event %d should have correct message", i)
	// 	// assert.Equal(t, uint64(i), event.Version, "Event %d should have correct stream version", i)
	// }

	t.Logf("Successfully saved and verified 100 events")
}

func TestOptimisticConcurrency(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)

	eventId, err := uuid.NewV7()
	require.NoError(t, err)

	// First save succeeds
	events := []orisun.EventWithMapTags{
		{
			EventId:   eventId.String(),
			EventType: "TestEvent",
			Data:      "{\"key\": \"value\"}",
		},
	}

	position1 := orisun.NotExistsPosition()
	_, _, err = saveEvents.Save(
		t.Context(),
		events,
		"test_boundary",
		&position1,
		&orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "key", Value: "value"},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	// Second save with wrong expected version should fail
	events2 := []orisun.EventWithMapTags{
		{
			EventId:   string(eventId.String()),
			EventType: "TestEvent",
			Data:      "{\"key\": \"value\"}",
			Metadata:  "{}",
		},
	}

	position2 := orisun.NotExistsPosition()
	_, _, err = saveEvents.Save(
		t.Context(),
		events2,
		"test_boundary",
		&position2,
		&orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "key", Value: "value"},
					},
				},
			},
		},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OptimisticConcurrencyException")
}

func TestConcurrentSaveEventsOptimisticConcurrency(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	// Create separate database connections for each operation to ensure true concurrency
	db2, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db2.Close()

	saveEvents1 := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	saveEvents2 := NewPostgresSaveEvents(t.Context(), db2, logger, mapping)

	// Create a shared stream consistency condition (Query) for both operations
	sharedStreamCondition := &orisun.Query{
		Criteria: []*orisun.Criterion{
			{
				Tags: []*orisun.Tag{
					{
						Key:   "test_type",
						Value: "concurrent_save",
					},
				},
			},
		},
	}

	// Pre-generate event IDs to avoid any timing issues
	eventId1, err := uuid.NewV7()
	require.NoError(t, err)
	eventId2, err := uuid.NewV7()
	require.NoError(t, err)

	events1 := []orisun.EventWithMapTags{
		{
			EventId:   eventId1.String(),
			EventType: "ConcurrentTestEvent1",
			Data:      "{\"operation\": \"first\", \"test_type\": \"concurrent_save\"}",
			Metadata:  "{\"test\": \"concurrent_save_1\"}",
		},
	}

	events2 := []orisun.EventWithMapTags{
		{
			EventId:   eventId2.String(),
			EventType: "ConcurrentTestEvent2",
			Data:      "{\"operation\": \"second\", \"test_type\": \"concurrent_save\"}",
			Metadata:  "{\"test\": \"concurrent_save_2\"}",
		},
	}

	// Use WaitGroup and a barrier to ensure both operations start simultaneously
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []error
	var concurrencyError error

	// Create a barrier to synchronize the start of both operations
	startBarrier := make(chan struct{})

	wg.Add(2)

	// Start first operation
	go func() {
		defer wg.Done()
		// Wait for the start signal
		<-startBarrier
		_, _, err := saveEvents1.Save(
			context.Background(),
			events1,
			"test_boundary",
			nil,
			sharedStreamCondition,
		)
		t.Logf("Operation 1 result: %v", err)
		mu.Lock()
		results = append(results, err)
		if err != nil && strings.Contains(err.Error(), "OptimisticConcurrencyException") {
			concurrencyError = err
		}
		mu.Unlock()
	}()

	// Start second operation
	go func() {
		defer wg.Done()
		// Wait for the start signal
		<-startBarrier
		_, _, err := saveEvents2.Save(
			context.Background(),
			events2,
			"test_boundary",
			nil,
			sharedStreamCondition,
		)
		t.Logf("Operation 2 result: %v", err)
		mu.Lock()
		results = append(results, err)
		if err != nil && strings.Contains(err.Error(), "OptimisticConcurrencyException") {
			concurrencyError = err
		}
		mu.Unlock()
	}()

	// Give both goroutines a moment to reach the barrier
	time.Sleep(10 * time.Millisecond)

	// Signal both operations to start simultaneously
	close(startBarrier)

	// Wait for both operations to complete
	wg.Wait()

	// Debug logging
	t.Logf("Operation 1 result: %v", results[0])
	t.Logf("Operation 2 result: %v", results[1])

	// One operation should succeed, one should fail with OptimisticConcurrencyException
	var successCount, failureCount int

	for i, err := range results {
		if err == nil {
			successCount++
			t.Logf("Operation %d: SUCCESS", i+1)
		} else {
			failureCount++
			t.Logf("Operation %d: FAILED with error: %v", i+1, err)
		}
	}

	t.Logf("Final counts - Success: %d, Failure: %d", successCount, failureCount)

	// Assertions
	assert.Equal(t, 1, successCount, "Exactly one save operation should succeed")
	assert.Equal(t, 1, failureCount, "Exactly one save operation should fail")
	assert.NotNil(t, concurrencyError, "There should be a concurrency error")
	if concurrencyError != nil {
		assert.Contains(t, concurrencyError.Error(), "OptimisticConcurrencyException", "The error should be an OptimisticConcurrencyException")
	}
}

func TestGetEventsWithCriteria(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)

	eventId, err := uuid.NewV7()
	require.NoError(t, err)
	// Save events with different tags
	events1 := []orisun.EventWithMapTags{
		{
			EventId:   eventId.String(),
			EventType: "TestEvent",
			Data:      "{\"key\": \"key\", \"value\": \"value1\"}",
			Metadata:  "{\"tags\": [{\"key\": \"key\", \"value\": \"value1\"}]}",
		},
	}

	expectedPosition := orisun.NotExistsPosition()
	tranId, globalId, err := saveEvents.Save(
		t.Context(),
		events1,
		"test_boundary",
		&expectedPosition,
		nil,
	)
	require.NoError(t, err)

	events2 := []orisun.EventWithMapTags{
		{
			EventId:   eventId.String(),
			EventType: "TestEvent",
			Data:      "{\"key\": \"value2\"}",
			Metadata:  "{}",
		},
	}

	tranIdConv, err := strconv.ParseInt(tranId, 10, 64)

	if err != nil {
		fmt.Printf("Error converting string to int64: %v\n", err)
		return
	}
	_, _, err = saveEvents.Save(
		t.Context(),
		events2,
		"test_boundary",
		&orisun.Position{
			PreparePosition: globalId,
			CommitPosition:  tranIdConv,
		},
		nil,
	)
	require.NoError(t, err)

	// Test filtering by tag criteria
	resp, err := getEvents.GetBatch(
		t.Context(),
		&orisun.GetEventsRequest{
			Boundary:  "test_boundary",
			Direction: orisun.Direction_ASC,
			Count:     10,
			Query: &orisun.Query{
				Criteria: []*orisun.Criterion{
					{
						Tags: []*orisun.Tag{
							{Key: "key", Value: "value2"},
						},
					},
				},
			},
		},
	)

	assert.NoError(t, err)
	assert.Len(t, resp, 1, "Expected to find one event with tag key=value2")
	if len(resp) > 0 {
		assert.Equal(t, eventId.String(), resp[0].EventId)
	}
}

func TestGetEventsByGlobalPosition(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)
	ctx := t.Context()

	// Save multiple events to get different global positions
	var globalPositions []int64
	var transactionIDs []string
	var lastPosition *orisun.Position
	for i := range 5 {
		eventId, err := uuid.NewV7()
		require.NoError(t, err)

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "TestEvent",
				Data:      fmt.Sprintf("{\"index\": %d}", i),
			},
		}

		transactionID, globalPos, err := saveEvents.Save(
			ctx,
			events,
			"test_boundary",
			lastPosition,
			nil,
		)
		require.NoError(t, err)
		globalPositions = append(globalPositions, globalPos)
		transactionIDs = append(transactionIDs, transactionID)
		transactionIDInt, err := strconv.ParseInt(transactionID, 10, 64)
		require.NoError(t, err)
		lastPosition = &orisun.Position{
			CommitPosition:  transactionIDInt,
			PreparePosition: globalPos,
		}
	}

	// Get events after the second event's global position
	// Position requires actual transaction_id and global_id from the saved events
	transactionIDInt, _ := strconv.ParseInt(transactionIDs[2], 10, 64)
	resp, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
		FromPosition: &orisun.Position{
			CommitPosition:  transactionIDInt,   // actual transaction_id
			PreparePosition: globalPositions[2], // actual global_id
		},
	})

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(resp), 3) // Should get at least events 2, 3, and 4

	for i, event := range resp {
		expectedIndex := i + 2
		var data map[string]any
		require.NoError(t, json.Unmarshal([]byte(event.Data), &data))
		assert.Equal(t, float64(expectedIndex), data["index"])
		assert.Equal(t, "TestEvent", data["eventType"])
	}
}

func TestPagination(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)
	ctx := t.Context()

	// Save 10 events
	var lastPosition *orisun.Position
	for i := range 10 {
		eventId, err := uuid.NewV7()
		require.NoError(t, err)

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "TestEvent",
				Data:      fmt.Sprintf("{\"index\": %d}", i),
			},
		}

		transactionID, globalPos, err := saveEvents.Save(
			ctx,
			events,
			"test_boundary",
			lastPosition,
			nil,
		)
		require.NoError(t, err)
		transactionIDInt, err := strconv.ParseInt(transactionID, 10, 64)
		require.NoError(t, err)
		lastPosition = &orisun.Position{
			CommitPosition:  transactionIDInt,
			PreparePosition: globalPos,
		}
	}

	// Get first page (3 events)
	resp1, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     3,
	})

	assert.NoError(t, err)
	assert.Len(t, resp1, 3)

	// Get second page (3 events) using composite position of last from page 1
	lastPos := resp1[len(resp1)-1]
	resp2, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     3,
		FromPosition: &orisun.Position{
			CommitPosition:  lastPos.CommitPosition,
			PreparePosition: lastPos.PreparePosition,
		},
	})

	assert.NoError(t, err)
	assert.Len(t, resp2, 3)

	// Verify the events are different between pages
	assert.NotEqual(t, resp1[0].EventId, resp2[0].EventId)
	assert.NotEqual(t, resp1[1].EventId, resp2[1].EventId)
	assert.NotEqual(t, resp1[2].EventId, resp2[2].EventId)
}

func TestDirectionOrdering(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)
	ctx := t.Context()

	// Save 5 events
	var lastPosition *orisun.Position
	for i := range 5 {
		eventId, err := uuid.NewV7()
		require.NoError(t, err)

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "TestEvent",
				Data:      fmt.Sprintf("{\"index\": %d}", i),
			},
		}

		transactionID, globalPos, err := saveEvents.Save(
			ctx,
			events,
			"test_boundary",
			lastPosition,
			nil,
		)
		require.NoError(t, err)
		transactionIDInt, err := strconv.ParseInt(transactionID, 10, 64)
		require.NoError(t, err)
		lastPosition = &orisun.Position{
			CommitPosition:  transactionIDInt,
			PreparePosition: globalPos,
		}
	}

	// Get events in ascending order
	respAsc, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
	})

	assert.NoError(t, err)
	assert.Len(t, respAsc, 5)

	// Get events in descending order
	respDesc, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_DESC,
		Count:     10,
	})

	assert.NoError(t, err)
	assert.Len(t, respDesc, 5)

	// Verify the order is reversed
	for i := range 5 {
		assert.Equal(t, respAsc[i].EventId, respDesc[4-i].EventId)
	}
}

func TestCreateAndDropBoundaryIndex(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	adminDB := NewPostgresAdminDB(db, logger, "public", "test_boundary", mapping)
	ctx := t.Context()

	indexExists := func(indexName string) bool {
		var exists bool
		err := db.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1)",
			indexName,
		).Scan(&exists)
		require.NoError(t, err)
		return exists
	}

	t.Run("single field text index", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "user_id", []common.IndexField{
			{JsonKey: "user_id", ValueType: "text"},
		}, nil, "")
		require.NoError(t, err)
		assert.True(t, indexExists("test_boundary_user_id_idx"), "index should exist after creation")

		indexes, err := adminDB.ListBoundaryIndexes(ctx, "test_boundary")
		require.NoError(t, err)
		require.Len(t, indexes, 1)
		assert.Equal(t, "user_id", indexes[0].Name)
		assert.Equal(t, orisun.BoundaryIndexStateReady, indexes[0].State)
		require.Len(t, indexes[0].Fields, 1)
		assert.Equal(t, "user_id", indexes[0].Fields[0].JsonKey)

		index, err := adminDB.GetBoundaryIndex(ctx, "test_boundary", "user_id")
		require.NoError(t, err)
		assert.Equal(t, common.CombinatorAND, index.Combinator)

		err = adminDB.DropBoundaryIndex(ctx, "test_boundary", "user_id")
		require.NoError(t, err)
		assert.False(t, indexExists("test_boundary_user_id_idx"), "index should be gone after drop")
		_, err = adminDB.GetBoundaryIndex(ctx, "test_boundary", "user_id")
		assert.Equal(t, statuscode.NotFound, statuscode.CodeOf(err))
	})

	t.Run("composite index", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "cat_prio", []common.IndexField{
			{JsonKey: "category", ValueType: "text"},
			{JsonKey: "priority", ValueType: "text"},
		}, nil, "")
		require.NoError(t, err)
		assert.True(t, indexExists("test_boundary_cat_prio_idx"))

		require.NoError(t, adminDB.DropBoundaryIndex(ctx, "test_boundary", "cat_prio"))
		assert.False(t, indexExists("test_boundary_cat_prio_idx"))
	})

	t.Run("partial index with condition", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "placed_amount", []common.IndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []common.IndexCondition{
			{Key: "eventType", Operator: "=", Value: "OrderPlaced"},
		}, common.CombinatorAND)
		require.NoError(t, err)
		assert.True(t, indexExists("test_boundary_placed_amount_idx"))

		require.NoError(t, adminDB.DropBoundaryIndex(ctx, "test_boundary", "placed_amount"))
		assert.False(t, indexExists("test_boundary_placed_amount_idx"))
	})

	t.Run("unknown boundary returns error", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "nonexistent", "idx", []common.IndexField{
			{JsonKey: "id", ValueType: "text"},
		}, nil, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown boundary")
	})

	t.Run("no fields returns error", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "empty", nil, nil, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one field")
	})

	t.Run("invalid operator returns error", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "bad_op", []common.IndexField{
			{JsonKey: "id", ValueType: "text"},
		}, []common.IndexCondition{
			{Key: "eventType", Operator: "LIKE", Value: "Order%"},
		}, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid operator")
	})

	t.Run("invalid combinator returns error", func(t *testing.T) {
		err := adminDB.CreateBoundaryIndex(ctx, "test_boundary", "bad_comb", []common.IndexField{
			{JsonKey: "id", ValueType: "text"},
		}, []common.IndexCondition{
			{Key: "eventType", Operator: "=", Value: "Placed"},
		}, "XOR")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid combinator")
	})

	t.Run("drop unknown boundary returns error", func(t *testing.T) {
		err := adminDB.DropBoundaryIndex(ctx, "nonexistent", "idx")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown boundary")
	})
}

func TestComplexTagQueries(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("debug")
	require.NoError(t, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {
			Boundary: "test_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	getEvents := NewPostgresGetEvents(db, logger, mapping)
	ctx := t.Context()

	// Save events with different tag combinations
	eventIds := make([]string, 4)

	var lastPosition *orisun.Position

	// Event 1: category=A, priority=high, region=east
	eventId1, err := uuid.NewV7()
	require.NoError(t, err)
	eventIds[0] = eventId1.String()
	events1 := []orisun.EventWithMapTags{
		{
			EventId:   eventId1.String(),
			EventType: "TestEvent",
			Data:      "{\"data\": \"event1\", \"category\": \"A\", \"priority\": \"high\", \"region\": \"east\"}",
		},
	}
	transactionID, globalPos, err := saveEvents.Save(
		ctx,
		events1,
		"test_boundary",
		lastPosition,
		nil,
	)
	require.NoError(t, err)
	transactionIDInt, err := strconv.ParseInt(transactionID, 10, 64)
	require.NoError(t, err)
	lastPosition = &orisun.Position{
		CommitPosition:  transactionIDInt,
		PreparePosition: globalPos,
	}

	// Event 2: category=A, priority=low, region=west
	eventId2, err := uuid.NewV7()
	require.NoError(t, err)
	eventIds[1] = eventId2.String()
	events2 := []orisun.EventWithMapTags{
		{
			EventId:   eventId2.String(),
			EventType: "TestEvent",
			Data:      "{\"data\": \"event2\", \"category\": \"A\", \"priority\": \"low\", \"region\": \"west\"}",
		},
	}
	transactionID, globalPos, err = saveEvents.Save(
		ctx,
		events2,
		"test_boundary",
		lastPosition,
		nil,
	)
	require.NoError(t, err)
	transactionIDInt, err = strconv.ParseInt(transactionID, 10, 64)
	require.NoError(t, err)
	lastPosition = &orisun.Position{
		CommitPosition:  transactionIDInt,
		PreparePosition: globalPos,
	}

	// Event 3: category=B, priority=high, region=east
	eventId3, err := uuid.NewV7()
	require.NoError(t, err)
	eventIds[2] = eventId3.String()
	events3 := []orisun.EventWithMapTags{
		{
			EventId:   eventId3.String(),
			EventType: "TestEvent",
			Data:      "{\"data\": \"event3\", \"category\": \"B\", \"priority\": \"high\", \"region\": \"east\"}",
		},
	}
	transactionID, globalPos, err = saveEvents.Save(
		ctx,
		events3,
		"test_boundary",
		lastPosition,
		nil,
	)
	require.NoError(t, err)
	transactionIDInt, err = strconv.ParseInt(transactionID, 10, 64)
	require.NoError(t, err)
	lastPosition = &orisun.Position{
		CommitPosition:  transactionIDInt,
		PreparePosition: globalPos,
	}

	// Event 4: category=B, priority=low, region=west
	eventId4, err := uuid.NewV7()
	require.NoError(t, err)
	eventIds[3] = eventId4.String()
	events4 := []orisun.EventWithMapTags{
		{
			EventId:   eventId4.String(),
			EventType: "TestEvent",
			Data:      "{\"data\": \"event4\", \"category\": \"B\", \"priority\": \"low\", \"region\": \"west\"}",
		},
	}
	_, _, err = saveEvents.Save(
		ctx,
		events4,
		"test_boundary",

		lastPosition,
		nil,
	)
	require.NoError(t, err)

	// Test 1: OR query - category A OR category B with priority high
	resp1, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
		Query: &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "category", Value: "A"},
						{Key: "priority", Value: "high"},
					},
				},
				{
					Tags: []*orisun.Tag{
						{Key: "category", Value: "B"},
						{Key: "priority", Value: "high"},
					},
				},
			},
		},
	})

	assert.NoError(t, err)
	assert.Len(t, resp1, 2, "The query should return 0 events as the tags are in metadata, not in data")

	// Test 2: AND query - region east
	resp2, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
		Query: &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "region", Value: "east"},
					},
				},
			},
		},
	})

	assert.NoError(t, err)
	assert.Len(t, resp2, 2, "The query should return 0 events as the tags are in metadata, not in data")

	// Test 3: Complex query - (category A AND region west) OR (category B AND priority high)
	resp3, err := getEvents.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:  "test_boundary",
		Direction: orisun.Direction_ASC,
		Count:     10,
		Query: &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "category", Value: "A"},
						{Key: "region", Value: "west"},
					},
				},
				{
					Tags: []*orisun.Tag{
						{Key: "category", Value: "B"},
						{Key: "priority", Value: "high"},
					},
				},
			},
		},
	})

	assert.NoError(t, err)
	assert.Len(t, resp3, 2, "The query should return 0 events as the tags are in metadata, not in data")
}
