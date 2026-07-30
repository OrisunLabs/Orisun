package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	benchNumStreams      = 500
	benchEventsPerStream = 20
)

// setupBenchmarkDB starts a Postgres container, opens a connection, runs migrations,
// and returns the database and a teardown function.
func setupBenchmarkDB(b *testing.B) (*sql.DB, func()) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:17.5-alpine3.22",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		Cmd: []string{
			"postgres",
			"-c", "shared_buffers=2GB",
			"-c", "work_mem=64MB",
			"-c", "maintenance_work_mem=512MB",
			"-c", "effective_cache_size=6GB",
			"-c", "wal_buffers=64MB",
			"-c", "min_wal_size=2GB",
			"-c", "max_wal_size=8GB",
			"-c", "checkpoint_completion_target=0.7",
			"-c", "checkpoint_timeout=3min",
			"-c", "max_connections=300",
			"-c", "max_worker_processes=14",
			"-c", "max_parallel_workers=14",
			"-c", "max_parallel_workers_per_gather=7",
			"-c", "max_parallel_maintenance_workers=7",
			"-c", "random_page_cost=1.1",
			"-c", "seq_page_cost=1.0",
			"-c", "cpu_tuple_cost=0.01",
			"-c", "cpu_index_tuple_cost=0.005",
			"-c", "cpu_operator_cost=0.0025",
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
	require.NoError(b, err, "failed to start container")

	// Give container a moment to fully initialize
	time.Sleep(2 * time.Second)

	host, err := container.Host(ctx)
	require.NoError(b, err, "failed to get container host")

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(b, err, "failed to get container port")

	connStr := fmt.Sprintf(
		"host=%s port=%s user=test password=test dbname=testdb sslmode=disable",
		host,
		port.Port(),
	)

	var db *sql.DB
	for retries := 0; retries < 3; retries++ {
		db, err = sql.Open("pgx", connStr)
		require.NoError(b, err, "failed to open database")

		db.SetMaxOpenConns(250)
		db.SetMaxIdleConns(100)
		db.SetConnMaxLifetime(time.Hour)

		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(pingCtx)
		cancel()

		if err == nil {
			break
		}
		db.Close()
		time.Sleep(time.Duration(retries+1) * time.Second)
	}
	require.NoError(b, err, "failed to ping database after retries")

	// Run migrations
	if err := RunDbScripts(db, "bench_boundary", "public", false, ctx); err != nil {
		db.Close()
		container.Terminate(ctx)
		require.NoError(b, err, "failed to run database migrations")
	}

	teardown := func() {
		db.Close()
		if err := container.Terminate(ctx); err != nil {
			b.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, teardown
}

// prepopulateStreams creates and saves events for multiple streams.
// Returns the stream IDs and their last known positions (as pointers).
func prepopulateStreams(
	b *testing.B,
	ctx context.Context,
	saveEvents *PostgresSaveEvents,
	boundary string,
	numStreams, eventsPerStream int,
) (streamIds []string, lastPositions []*orisun.Position) {
	streamIds = make([]string, numStreams)
	lastPositions = make([]*orisun.Position, numStreams)

	// Generate stream IDs
	for i := 0; i < numStreams; i++ {
		streamId, err := uuid.NewV7()
		require.NoError(b, err)
		streamIds[i] = streamId.String()
	}

	// Pre-populate events for each stream
	for streamIdx := 0; streamIdx < numStreams; streamIdx++ {
		streamId := streamIds[streamIdx]
		pos := orisun.NotExistsPosition()

		for eventIdx := 0; eventIdx < eventsPerStream; eventIdx++ {
			eventId, err := uuid.NewV7()
			require.NoError(b, err)

			eventData := fmt.Sprintf(
				`{"stream_id": "%s", "eventType": "OrderPlaced", "sequence": %d}`,
				streamId,
				eventIdx,
			)

			metadata := fmt.Sprintf(`{"timestamp": "%s"}`, time.Now().Format(time.RFC3339))

			events := []orisun.EventWithMapTags{
				{
					EventId:   eventId.String(),
					EventType: "OrderPlaced",
					Data:      eventData,
					Metadata:  metadata,
				},
			}

			tranID, globalID, err := saveEvents.Save(ctx, events, boundary, &pos, nil)
			require.NoError(b, err, "failed to save pre-population event stream=%d event=%d", streamIdx, eventIdx)

			transactionIDInt, err := parseTransactionID(tranID)
			require.NoError(b, err)

			// Update position for next iteration
			pos = orisun.Position{
				PreparePosition: globalID,
				CommitPosition:  transactionIDInt,
			}
		}

		// Store final position for this stream
		lastPositions[streamIdx] = &pos
	}

	return streamIds, lastPositions
}

// parseTransactionID parses a transaction ID string to int64
func parseTransactionID(tranID string) (int64, error) {
	var tid int64
	_, err := fmt.Sscanf(tranID, "%d", &tid)
	return tid, err
}

// BenchmarkConsistencyCheck_NoIndex benchmarks the consistency check path
// WITHOUT a btree index - performs a sequential scan on the version check.
func BenchmarkConsistencyCheck_NoIndex(b *testing.B) {
	db, teardown := setupBenchmarkDB(b)
	defer teardown()

	ctx := context.Background()
	logger, err := logging.ZapLogger("warn") // Use warn to reduce noise in benchmarks
	require.NoError(b, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"bench_boundary": {
			Boundary: "bench_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(ctx, db, logger, mapping)

	// Pre-populate the database with events
	streamIds, lastPositions := prepopulateStreams(b, ctx, saveEvents, "bench_boundary", benchNumStreams, benchEventsPerStream)

	b.ResetTimer()

	// Benchmark loop: perform save operations with consistency checks
	for i := 0; i < b.N; i++ {
		// Pick a stream (round-robin)
		streamIdx := i % benchNumStreams
		streamId := streamIds[streamIdx]

		eventId, err := uuid.NewV7()
		require.NoError(b, err)

		eventData := fmt.Sprintf(
			`{"stream_id": "%s", "eventType": "OrderPlaced", "sequence": %d}`,
			streamId,
			benchEventsPerStream+i,
		)

		metadata := fmt.Sprintf(`{"timestamp": "%s"}`, time.Now().Format(time.RFC3339))

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "OrderPlaced",
				Data:      eventData,
				Metadata:  metadata,
			},
		}

		// Query with consistency condition - this is what triggers the version check
		query := &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "stream_id", Value: streamId},
					},
				},
			},
		}

		pos := lastPositions[streamIdx]
		tranID, globalID, err := saveEvents.Save(ctx, events, "bench_boundary", pos, query)
		require.NoError(b, err, "iteration %d failed", i)

		transactionIDInt, err := parseTransactionID(tranID)
		require.NoError(b, err)

		// Create new position and store it
		newPos := orisun.Position{
			PreparePosition: globalID,
			CommitPosition:  transactionIDInt,
		}
		lastPositions[streamIdx] = &newPos
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "saves/sec")
}

// BenchmarkConsistencyCheck_WithIndex benchmarks the consistency check path
// WITH a btree index on stream_id - performs an indexed lookup on the version check.
func BenchmarkConsistencyCheck_WithIndex(b *testing.B) {
	db, teardown := setupBenchmarkDB(b)
	defer teardown()

	ctx := context.Background()
	logger, err := logging.ZapLogger("warn")
	require.NoError(b, err)

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"bench_boundary": {
			Boundary: "bench_boundary",
			Schema:   "public",
		},
	}

	saveEvents := NewPostgresSaveEvents(ctx, db, logger, mapping)
	adminDB := NewPostgresAdminDB(db, logger, "public", "bench_boundary", mapping)

	// Pre-populate the database with events
	streamIds, lastPositions := prepopulateStreams(b, ctx, saveEvents, "bench_boundary", benchNumStreams, benchEventsPerStream)

	// Create the index AFTER pre-population (as specified in plan)
	err = adminDB.CreateBoundaryIndex(ctx, "bench_boundary", "stream_id", []common.IndexField{
		{JsonKey: "stream_id", ValueType: "text"},
	}, nil, "")
	require.NoError(b, err, "failed to create index")

	b.ResetTimer()

	// Benchmark loop: perform save operations with consistency checks
	for i := 0; i < b.N; i++ {
		// Pick a stream (round-robin)
		streamIdx := i % benchNumStreams
		streamId := streamIds[streamIdx]

		eventId, err := uuid.NewV7()
		require.NoError(b, err)

		eventData := fmt.Sprintf(
			`{"stream_id": "%s", "eventType": "OrderPlaced", "sequence": %d}`,
			streamId,
			benchEventsPerStream+i,
		)

		metadata := fmt.Sprintf(`{"timestamp": "%s"}`, time.Now().Format(time.RFC3339))

		events := []orisun.EventWithMapTags{
			{
				EventId:   eventId.String(),
				EventType: "OrderPlaced",
				Data:      eventData,
				Metadata:  metadata,
			},
		}

		// Query with consistency condition - this is what triggers the version check
		query := &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "stream_id", Value: streamId},
					},
				},
			},
		}

		pos := lastPositions[streamIdx]
		tranID, globalID, err := saveEvents.Save(ctx, events, "bench_boundary", pos, query)
		require.NoError(b, err, "iteration %d failed", i)

		transactionIDInt, err := parseTransactionID(tranID)
		require.NoError(b, err)

		// Create new position and store it
		newPos := orisun.Position{
			PreparePosition: globalID,
			CommitPosition:  transactionIDInt,
		}
		lastPositions[streamIdx] = &newPos
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "saves/sec")
}

// BenchmarkPostgres_Burst10000 fires a fixed-size burst of concurrent
// single-event appends. Run with -benchtime=1x when you want wall-clock burst
// throughput.
func BenchmarkPostgres_Burst10000(b *testing.B) {
	const burst = 10000

	for _, maxConns := range []int{10, 25, 50, 100, 250} {
		b.Run(fmt.Sprintf("pool_%d", maxConns), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db, teardown := setupBenchmarkDB(b)
				db.SetMaxOpenConns(maxConns)
				db.SetMaxIdleConns(maxConns)

				ctx := context.Background()
				logger, err := logging.ZapLogger("warn")
				require.NoError(b, err)
				mapping := map[string]config.BoundaryToPostgresSchemaMapping{
					"bench_boundary": {
						Boundary: "bench_boundary",
						Schema:   "public",
					},
				}
				saveEvents := NewPostgresSaveEvents(ctx, db, logger, mapping)

				events := make([]orisun.EventWithMapTags, burst)
				for j := 0; j < burst; j++ {
					id, err := uuid.NewV7()
					require.NoError(b, err)
					events[j] = orisun.EventWithMapTags{
						EventId:   id.String(),
						EventType: "BurstEvent",
						Data:      `{"k":"v"}`,
						Metadata:  `{}`,
					}
				}

				var wg sync.WaitGroup
				var ok, fail int64
				startCh := make(chan struct{})
				wg.Add(burst)
				for j := 0; j < burst; j++ {
					ev := events[j]
					go func() {
						defer wg.Done()
						<-startCh
						if _, _, err := saveEvents.Save(ctx, []orisun.EventWithMapTags{ev}, "bench_boundary", nil, nil); err != nil {
							atomic.AddInt64(&fail, 1)
							return
						}
						atomic.AddInt64(&ok, 1)
					}()
				}

				b.ResetTimer()
				startTime := time.Now()
				close(startCh)
				wg.Wait()
				elapsed := time.Since(startTime)
				b.StopTimer()

				b.ReportMetric(float64(ok)/elapsed.Seconds(), "events/sec")
				b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")
				if fail > 0 {
					b.Logf("burst had %d failures", fail)
				}

				teardown()
			}
		})
	}
}

// BenchmarkPostgres_GroupCommitBatchSize measures the end-to-end concurrent
// SaveEvents path while sweeping the maximum set-based flush size.
func BenchmarkPostgres_GroupCommitBatchSize(b *testing.B) {
	const burst = 10000

	for _, batchSize := range []int{64, 128, 256, 512, 1024, 2048} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			for range b.N {
				db, teardown := setupBenchmarkDB(b)
				ctx := context.Background()
				logger, err := logging.ZapLogger("warn")
				require.NoError(b, err)
				mapping := map[string]config.BoundaryToPostgresSchemaMapping{
					"bench_boundary": {
						Boundary: "bench_boundary",
						Schema:   "public",
					},
				}
				saveEvents, err := NewPostgresSaveEventsWithConfig(
					ctx,
					db,
					logger,
					mapping,
					config.PostgresGroupCommitConfig{
						MaxBatchRequests: batchSize,
						MaxBatchEvents:   batchSize,
						MaxPending:       burst,
					},
				)
				require.NoError(b, err)

				events := make([]orisun.EventWithMapTags, burst)
				for i := range events {
					eventID, err := uuid.NewV7()
					require.NoError(b, err)
					events[i] = orisun.EventWithMapTags{
						EventId:   eventID.String(),
						EventType: "BurstEvent",
						Data:      `{"k":"v"}`,
						Metadata:  `{}`,
					}
				}

				var wg sync.WaitGroup
				var succeeded, failed atomic.Int64
				start := make(chan struct{})
				wg.Add(burst)
				for i := range events {
					event := events[i]
					go func() {
						defer wg.Done()
						<-start
						if _, _, err := saveEvents.Save(
							ctx,
							[]orisun.EventWithMapTags{event},
							"bench_boundary",
							nil,
							nil,
						); err != nil {
							failed.Add(1)
							return
						}
						succeeded.Add(1)
					}()
				}

				b.ResetTimer()
				startedAt := time.Now()
				close(start)
				wg.Wait()
				elapsed := time.Since(startedAt)
				b.StopTimer()

				b.ReportMetric(float64(succeeded.Load())/elapsed.Seconds(), "events/sec")
				b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")
				require.Zero(b, failed.Load())
				require.Equal(b, int64(burst), succeeded.Load())

				saveEvents.close()
				teardown()
			}
		})
	}
}

// BenchmarkPostgres_GroupCommitCCC10000 measures concurrent independent CCC
// contexts. No request conflicts because every content query targets a distinct
// value of the same indexed key.
func BenchmarkPostgres_GroupCommitCCC10000(b *testing.B) {
	benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{}, false, false, 1)
}

func BenchmarkPostgres_GroupCommitCCCMultiEvent10000(b *testing.B) {
	benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
		MaxBatchRequests: 512,
		MaxBatchEvents:   1024,
		MaxDelay:         time.Millisecond,
	}, false, false, 2)
}

// BenchmarkPostgres_GroupCommitGeneralCCC10000 forces the general criterion
// state resolver with mixed keys, two-tag AND, and two-criterion OR queries.
func BenchmarkPostgres_GroupCommitGeneralCCC10000(b *testing.B) {
	benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{}, true, true, 1)
}

// BenchmarkPostgres_GroupCommitGeneralCCCMultiEvent10000 measures 10,000
// concurrent mixed-shape CCC saves with two events in every accepted request.
func BenchmarkPostgres_GroupCommitGeneralCCCMultiEvent10000(b *testing.B) {
	benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
		MaxBatchRequests: 512,
		MaxBatchEvents:   1024,
		MaxDelay:         time.Millisecond,
	}, true, true, 2)
}

func BenchmarkPostgres_GroupCommitGeneralCCCMultiEventBatchSize(b *testing.B) {
	const burst = 10000
	for _, batchSize := range []int{128, 256, 512, 1024} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
				MaxBatchRequests: batchSize,
				MaxBatchEvents:   batchSize * 2,
				MaxPending:       burst,
			}, true, true, 2)
		})
	}
}

func BenchmarkPostgres_GroupCommitGeneralCCCMultiEventDelay(b *testing.B) {
	const burst = 10000
	for _, delay := range []time.Duration{
		0,
		100 * time.Microsecond,
		250 * time.Microsecond,
		500 * time.Microsecond,
		time.Millisecond,
	} {
		b.Run(delay.String(), func(b *testing.B) {
			benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
				MaxBatchRequests: 512,
				MaxBatchEvents:   1024,
				MaxDelay:         delay,
				MaxPending:       burst,
			}, true, true, 2)
		})
	}
}

func BenchmarkPostgres_GroupCommitMixedCCCBatchSize(b *testing.B) {
	const burst = 10000
	for _, batchSize := range []int{128, 256, 512, 1024} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
				MaxBatchRequests: batchSize,
				MaxBatchEvents:   batchSize,
				MaxPending:       burst,
			}, true, true, 1)
		})
	}
}

func BenchmarkPostgres_GroupCommitGeneralCCCBatchSize(b *testing.B) {
	const burst = 10000
	for _, batchSize := range []int{16, 32, 64, 128, 256, 512} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
				MaxBatchRequests: batchSize,
				MaxBatchEvents:   batchSize,
				MaxPending:       burst,
			}, true, false, 1)
		})
	}
}

// BenchmarkPostgres_GroupCommitCCCBatchSize sweeps the set-based CCC batch
// size. Run with -benchtime=1x to measure one 10,000-request burst per size.
func BenchmarkPostgres_GroupCommitCCCBatchSize(b *testing.B) {
	const burst = 10000
	for _, batchSize := range []int{64, 128, 256, 512, 1024, 2048} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkPostgresGroupCommitCCC10000(b, config.PostgresGroupCommitConfig{
				MaxBatchRequests: batchSize,
				MaxBatchEvents:   batchSize,
				MaxPending:       burst,
			}, false, false, 1)
		})
	}
}

func benchmarkPostgresGroupCommitCCC10000(
	b *testing.B,
	groupCommitConfig config.PostgresGroupCommitConfig,
	general bool,
	mixedShapes bool,
	eventsPerSave int,
) {
	const burst = 10000
	if groupCommitConfig.MaxPending == 0 {
		groupCommitConfig.MaxPending = burst
	}

	for range b.N {
		db, teardown := setupBenchmarkDB(b)
		ctx := context.Background()
		logger, err := logging.ZapLogger("warn")
		require.NoError(b, err)
		mapping := map[string]config.BoundaryToPostgresSchemaMapping{
			"bench_boundary": {
				Boundary: "bench_boundary",
				Schema:   "public",
			},
		}
		saveEvents, err := NewPostgresSaveEventsWithConfig(
			ctx,
			db,
			logger,
			mapping,
			groupCommitConfig,
		)
		require.NoError(b, err)
		adminDB := NewPostgresAdminDB(db, logger, "public", "bench_boundary", mapping)
		indexes := map[string][]common.IndexField{
			"stream_context": {{JsonKey: "stream_id", ValueType: "text"}},
		}
		if general {
			indexes["stream_context"] = append(indexes["stream_context"], common.IndexField{
				JsonKey: "tenant_id", ValueType: "text",
			})
		}
		if mixedShapes {
			indexes = map[string][]common.IndexField{
				"account_context": {{JsonKey: "account_id", ValueType: "text"}},
				"stream_context": {
					{JsonKey: "stream_id", ValueType: "text"},
					{JsonKey: "tenant_id", ValueType: "text"},
				},
				"order_context":     {{JsonKey: "order_id", ValueType: "text"}},
				"alternate_context": {{JsonKey: "alternate_id", ValueType: "text"}},
			}
		}
		for indexName, indexFields := range indexes {
			require.NoError(b, adminDB.CreateBoundaryIndex(
				ctx,
				"bench_boundary",
				indexName,
				indexFields,
				nil,
				"",
			))
		}

		events := make([][]orisun.EventWithMapTags, burst)
		queries := make([]*orisun.Query, burst)
		for i := range burst {
			streamID := fmt.Sprintf("stream-%d", i)
			eventData := fmt.Sprintf(`{"stream_id":%q,"tenant_id":"benchmark"}`, streamID)
			tags := []*orisun.Tag{{Key: "stream_id", Value: streamID}}
			criteria := []*orisun.Criterion{{Tags: tags}}
			if general {
				tags = append(tags, &orisun.Tag{Key: "tenant_id", Value: "benchmark"})
				criteria = []*orisun.Criterion{{Tags: tags}}
			}
			if mixedShapes {
				switch i % 3 {
				case 0:
					eventData = fmt.Sprintf(`{"account_id":%q}`, streamID)
					criteria = []*orisun.Criterion{{Tags: []*orisun.Tag{
						{Key: "account_id", Value: streamID},
					}}}
				case 1:
					eventData = fmt.Sprintf(
						`{"stream_id":%q,"tenant_id":"benchmark"}`,
						streamID,
					)
					criteria = []*orisun.Criterion{{Tags: []*orisun.Tag{
						{Key: "stream_id", Value: streamID},
						{Key: "tenant_id", Value: "benchmark"},
					}}}
				case 2:
					eventData = fmt.Sprintf(`{"order_id":%q}`, streamID)
					criteria = []*orisun.Criterion{
						{Tags: []*orisun.Tag{{Key: "order_id", Value: streamID}}},
						{Tags: []*orisun.Tag{{
							Key:   "alternate_id",
							Value: "missing-" + streamID,
						}}},
					}
				}
			}
			events[i] = make([]orisun.EventWithMapTags, eventsPerSave)
			for eventIndex := range eventsPerSave {
				eventID, err := uuid.NewV7()
				require.NoError(b, err)
				events[i][eventIndex] = orisun.EventWithMapTags{
					EventId:   eventID.String(),
					EventType: fmt.Sprintf("BurstCCCEvent%d", eventIndex),
					Data:      eventData,
					Metadata:  `{}`,
				}
			}
			queries[i] = &orisun.Query{Criteria: criteria}
		}
		notExists := orisun.NotExistsPosition()

		var wg sync.WaitGroup
		var succeeded, failed atomic.Int64
		start := make(chan struct{})
		wg.Add(burst)
		for i := range events {
			eventBatch := events[i]
			query := queries[i]
			go func() {
				defer wg.Done()
				<-start
				if _, _, err := saveEvents.Save(
					ctx,
					eventBatch,
					"bench_boundary",
					&notExists,
					query,
				); err != nil {
					failed.Add(1)
					return
				}
				succeeded.Add(1)
			}()
		}

		b.ResetTimer()
		startedAt := time.Now()
		close(start)
		wg.Wait()
		elapsed := time.Since(startedAt)
		b.StopTimer()

		b.ReportMetric(float64(succeeded.Load())/elapsed.Seconds(), "saves/sec")
		b.ReportMetric(
			float64(succeeded.Load()*int64(eventsPerSave))/elapsed.Seconds(),
			"events/sec",
		)
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")
		require.Zero(b, failed.Load())
		require.Equal(b, int64(burst), succeeded.Load())

		saveEvents.close()
		teardown()
	}
}
