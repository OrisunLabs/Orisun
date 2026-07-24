package sqlite

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/OrisunLabs/Orisun/admin"
	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
)

const (
	benchBoundary        = "bench_boundary"
	benchNumStreams      = 500
	benchEventsPerStream = 20
)

// setupBenchmarkPools opens a single-boundary pool set against a temp directory
// using SQLite's durable FULL synchronous mode.
// Returns the SqliteSaveEvents/SqliteAdminDB and a teardown closure.
func setupBenchmarkPools(b *testing.B) (*SqliteSaveEvents, *SqliteGetEvents, *SqliteAdminDB, func()) {
	return setupBenchmarkPoolsWithSynchronous(b, "FULL")
}

func setupBenchmarkPoolsWithSynchronous(b *testing.B, synchronous string) (*SqliteSaveEvents, *SqliteGetEvents, *SqliteAdminDB, func()) {
	b.Helper()
	dir := b.TempDir()
	logger, err := logging.ZapLogger("warn")
	require.NoError(b, err)

	bp, err := OpenBoundaryPoolsWithConfig(context.Background(), config.SqliteConfig{
		Dir:         dir,
		Synchronous: synchronous,
	}, benchBoundary, benchBoundary)
	require.NoError(b, err, "open pools")
	pools := map[string]*BoundaryPools{benchBoundary: bp}

	saver := NewSqliteSaveEvents(pools, logger)
	getter := NewSqliteGetEvents(pools, logger)
	admin := NewSqliteAdminDB(pools, benchBoundary, logger)

	return saver, getter, admin, func() {
		saver.close()
		_ = bp.Close()
	}
}

type benchFakeJetStream struct {
	jetstream.JetStream
}

func (benchFakeJetStream) CreateOrUpdateStream(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
	return nil, nil
}

func (benchFakeJetStream) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{}, nil
}

type benchNoopLockProvider struct{}

func (benchNoopLockProvider) Lock(context.Context, string) error { return nil }

type benchNoopPublishingTracker struct{}

func (benchNoopPublishingTracker) GetLastPublishedEventPosition(context.Context, string) (orisun.Position, error) {
	return orisun.NotExistsPosition(), nil
}

func (benchNoopPublishingTracker) InsertLastPublishedEvent(context.Context, string, int64, int64) error {
	return nil
}

// benchSaveFn picks the write path for a benchmark mode: "group" is the real
// Save API (always batched); "direct" bypasses the queue via the package
// helper to measure the pre-group-commit per-request transaction baseline.
func benchSaveFn(saver *SqliteSaveEvents, mode string) func(context.Context, []orisun.EventWithMapTags, string, *orisun.Position, *orisun.Query) (string, int64, error) {
	if mode == "direct" {
		return func(ctx context.Context, events []orisun.EventWithMapTags, boundary string, pos *orisun.Position, query *orisun.Query) (string, int64, error) {
			return saveBypassingQueue(saver, ctx, events, boundary, pos, query)
		}
	}
	return saver.Save
}

// BenchmarkSqlite_GroupCommitVsDirect pits the batched write path against the
// per-request transaction path under FULL synchronous — the configuration
// group commit is meant to pay for. Independent contexts (no CCC).
func BenchmarkSqlite_GroupCommitVsDirect(b *testing.B) {
	for _, mode := range []string{"group", "direct"} {
		for _, conc := range []int{1, 16, 100} {
			b.Run(fmt.Sprintf("mode=%s/workers=%d", mode, conc), func(b *testing.B) {
				saver, _, _, teardown := setupBenchmarkPools(b)
				defer teardown()
				save := benchSaveFn(saver, mode)

				ctx := context.Background()
				b.ResetTimer()

				var done int64
				var wg sync.WaitGroup
				startCh := make(chan struct{})
				start := time.Now()
				for w := 0; w < conc; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-startCh
						for {
							i := atomic.AddInt64(&done, 1)
							if i > int64(b.N) {
								return
							}
							id, _ := uuid.NewV7()
							_, _, err := save(ctx, []orisun.EventWithMapTags{{
								EventId:   id.String(),
								EventType: "Bench",
								Data:      `{"k":"v"}`,
								Metadata:  `{}`,
							}}, benchBoundary, nil, nil)
							if err != nil {
								b.Errorf("save failed: %v", err)
								return
							}
						}
					}()
				}
				close(startCh)
				wg.Wait()
				b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "events/sec")
			})
		}
	}
}

// BenchmarkSqlite_GroupCommitDelay sweeps gcMaxDelay to show its throughput /
// latency trade: a nonzero delay makes the worker wait to fill batches, which
// can help sustained high concurrency but directly taxes the lone writer —
// every solo save waits out the full delay before its flush.
func BenchmarkSqlite_GroupCommitDelay(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Millisecond, 10 * time.Millisecond} {
		for _, conc := range []int{1, 100} {
			b.Run(fmt.Sprintf("delay=%s/workers=%d", delay, conc), func(b *testing.B) {
				saver, _, _, teardown := setupBenchmarkPools(b)
				defer teardown()
				saver.gcMaxDelay = delay

				ctx := context.Background()
				b.ResetTimer()

				var done int64
				var wg sync.WaitGroup
				startCh := make(chan struct{})
				start := time.Now()
				for w := 0; w < conc; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-startCh
						for {
							i := atomic.AddInt64(&done, 1)
							if i > int64(b.N) {
								return
							}
							id, _ := uuid.NewV7()
							_, _, err := saver.Save(ctx, []orisun.EventWithMapTags{{
								EventId:   id.String(),
								EventType: "Bench",
								Data:      `{"k":"v"}`,
								Metadata:  `{}`,
							}}, benchBoundary, nil, nil)
							if err != nil {
								b.Errorf("save failed: %v", err)
								return
							}
						}
					}()
				}
				close(startCh)
				wg.Wait()
				b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "events/sec")
			})
		}
	}
}

// BenchmarkSqlite_GroupCommitVsDirect_CCC is the same comparison on the hot
// CCC path: every save carries a per-stream criterion and expected position,
// so batched requests exercise the savepoint + consistency-check loop.
//
// Per-save cost grows with table size (the CCC check scans the criterion's
// index slice), so cross-mode numbers are only comparable at equal iteration
// counts — run with a fixed -benchtime=Nx, not a duration.
func BenchmarkSqlite_GroupCommitVsDirect_CCC(b *testing.B) {
	for _, mode := range []string{"group", "direct"} {
		for _, conc := range []int{16, 100} {
			b.Run(fmt.Sprintf("mode=%s/workers=%d", mode, conc), func(b *testing.B) {
				saver, _, admin, teardown := setupBenchmarkPools(b)
				defer teardown()
				save := benchSaveFn(saver, mode)

				ctx := context.Background()
				streamIds, positions := prepopulateStreams(b, ctx, saver, conc, 5)
				require.NoError(b, admin.CreateBoundaryIndex(ctx, benchBoundary, "stream_id",
					[]common.IndexField{{JsonKey: "stream_id", ValueType: "text"}}, nil, ""))

				// One goroutine per stream: within a stream saves are causally
				// ordered (each carries the previous position), across streams
				// they are independent and can share a flush.
				perWorker := b.N / conc
				if perWorker == 0 {
					perWorker = 1
				}
				b.ResetTimer()
				var wg sync.WaitGroup
				startCh := make(chan struct{})
				start := time.Now()
				for w := 0; w < conc; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						<-startCh
						pos := positions[w]
						for i := 0; i < perWorker; i++ {
							id, _ := uuid.NewV7()
							data := fmt.Sprintf(`{"stream_id":"%s","eventType":"OrderPlaced","sequence":%d}`, streamIds[w], 5+i)
							query := &orisun.Query{Criteria: []*orisun.Criterion{{
								Tags: []*orisun.Tag{{Key: "stream_id", Value: streamIds[w]}},
							}}}
							tranID, gid, err := save(ctx, []orisun.EventWithMapTags{{
								EventId:   id.String(),
								EventType: "OrderPlaced",
								Data:      data,
								Metadata:  `{}`,
							}}, benchBoundary, pos, query)
							if err != nil {
								b.Errorf("save failed: %v", err)
								return
							}
							pos = &orisun.Position{CommitPosition: parseTxID(tranID), PreparePosition: gid}
						}
					}(w)
				}
				close(startCh)
				wg.Wait()
				b.ReportMetric(float64(perWorker*conc)/time.Since(start).Seconds(), "events/sec")
			})
		}
	}
}

// BenchmarkSqlite_EventStoreBurst10000 exercises the server-side SaveEvents
// handler directly, without gRPC transport. Compare this with
// BenchmarkSqlite_Burst10000 to isolate handler overhead, and with
// cmd.BenchmarkSaveEvents_Burst10000 to isolate gRPC/protobuf overhead.
func BenchmarkSqlite_EventStoreBurst10000(b *testing.B) {
	const burst = 10000

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		saver, getter, _, teardown := setupBenchmarkPools(b)
		logger, err := logging.ZapLogger("warn")
		require.NoError(b, err)
		server := orisun.NewEventStoreServer(
			benchFakeJetStream{},
			saver,
			getter,
			nil,
			nil,
			orisun.EventStreamConfig{},
			logger,
		)

		events := make([]*orisun.EventToSave, burst)
		for j := 0; j < burst; j++ {
			id, _ := uuid.NewV7()
			events[j] = &orisun.EventToSave{
				EventId:   id.String(),
				EventType: "BurstEvent",
				Data:      `{"k":"v"}`,
				Metadata:  `{}`,
			}
		}

		ctx := context.Background()
		var wg sync.WaitGroup
		var ok, fail int64
		startCh := make(chan struct{})
		wg.Add(burst)
		for j := 0; j < burst; j++ {
			ev := events[j]
			go func() {
				defer wg.Done()
				<-startCh
				if _, err := server.SaveEvents(ctx, &orisun.SaveEventsRequest{
					Boundary: benchBoundary,
					Events:   []*orisun.EventToSave{ev},
				}); err != nil {
					atomic.AddInt64(&fail, 1)
					return
				}
				atomic.AddInt64(&ok, 1)
			}()
		}

		b.StartTimer()
		startTime := time.Now()
		close(startCh)
		wg.Wait()
		elapsed := time.Since(startTime)
		b.StopTimer()

		if fail > 0 {
			b.Logf("burst had %d failures", fail)
		}
		b.ReportMetric(float64(ok)/elapsed.Seconds(), "events/sec")
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")
		teardown()
	}
}

// BenchmarkSqlite_GRPCEventStoreBurst10000 runs the same EventStore handler
// through grpc.Server/client on an in-memory listener. It isolates protobuf and
// gRPC dispatch overhead from the full cmd server's NATS publisher/projectors.
func BenchmarkSqlite_GRPCEventStoreBurst10000(b *testing.B) {
	const burst = 10000
	const bufSize = 100 * 1024 * 1024

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		saver, getter, _, teardown := setupBenchmarkPools(b)
		logger, err := logging.ZapLogger("warn")
		require.NoError(b, err)
		eventStore := orisun.NewEventStoreServer(
			benchFakeJetStream{},
			saver,
			getter,
			nil,
			nil,
			orisun.EventStreamConfig{},
			logger,
		)

		listener := bufconn.Listen(bufSize)
		grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(bufSize), grpc.MaxSendMsgSize(bufSize))
		grpcapi.RegisterEventStoreServer(grpcServer, grpcapi.AdaptEventStore(eventStore))
		serveErr := make(chan error, 1)
		go func() {
			serveErr <- grpcServer.Serve(listener)
		}()

		dialer := func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}
		conn, err := grpc.DialContext(
			context.Background(),
			"bufnet",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(bufSize)),
		)
		require.NoError(b, err)
		client := grpcapi.NewEventStoreClient(conn)

		events := make([]*grpcapi.EventToSave, burst)
		for j := 0; j < burst; j++ {
			id, _ := uuid.NewV7()
			events[j] = &grpcapi.EventToSave{
				EventId:   id.String(),
				EventType: "BurstEvent",
				Data:      `{"k":"v"}`,
				Metadata:  `{}`,
			}
		}

		ctx := context.Background()
		var wg sync.WaitGroup
		var ok, fail int64
		startCh := make(chan struct{})
		wg.Add(burst)
		for j := 0; j < burst; j++ {
			ev := events[j]
			go func() {
				defer wg.Done()
				<-startCh
				if _, err := client.SaveEvents(ctx, &grpcapi.SaveEventsRequest{
					Boundary: benchBoundary,
					Events:   []*grpcapi.EventToSave{ev},
				}); err != nil {
					atomic.AddInt64(&fail, 1)
					return
				}
				atomic.AddInt64(&ok, 1)
			}()
		}

		b.StartTimer()
		startTime := time.Now()
		close(startCh)
		wg.Wait()
		elapsed := time.Since(startTime)
		b.StopTimer()

		if fail > 0 {
			b.Logf("burst had %d failures", fail)
		}
		b.ReportMetric(float64(ok)/elapsed.Seconds(), "events/sec")
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")

		require.NoError(b, conn.Close())
		grpcServer.Stop()
		require.NoError(b, listener.Close())
		err = <-serveErr
		if err != nil {
			require.ErrorIs(b, err, grpc.ErrServerStopped)
		}
		teardown()
	}
}

type sqliteGRPCBenchClient struct {
	conn   *grpc.ClientConn
	client grpcapi.EventStoreClient
	ctx    context.Context
}

const (
	sqliteBenchGRPCMaxMessageSize  = 100 * 1024 * 1024
	sqliteBenchGRPCWindowSize      = 1024 * 1024
	sqliteBenchGRPCWriteBufferSize = 65536
	sqliteBenchGRPCReadBufferSize  = 65536
)

type benchmarkEventStoreServer struct {
	grpcapi.EventStoreServer
	getEvents common.GetEventsType
}

func newBenchmarkEventStoreServer(b *testing.B, saver *SqliteSaveEvents, getter *SqliteGetEvents) *benchmarkEventStoreServer {
	b.Helper()
	logger, err := logging.ZapLogger("warn")
	require.NoError(b, err)
	domain := orisun.NewEventStoreServer(
		benchFakeJetStream{},
		saver,
		getter,
		nil,
		nil,
		orisun.EventStreamConfig{},
		logger,
	)
	return &benchmarkEventStoreServer{
		EventStoreServer: grpcapi.AdaptEventStore(domain),
		getEvents:        domain.GetEvents,
	}
}

func sqliteGRPCServerOptions(b *testing.B, optionSet, authMode string, eventStore *benchmarkEventStoreServer) []grpc.ServerOption {
	b.Helper()
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(sqliteBenchGRPCMaxMessageSize),
		grpc.MaxSendMsgSize(sqliteBenchGRPCMaxMessageSize),
	}
	if optionSet == "cmd_like" {
		opts = append(opts,
			grpc.MaxConcurrentStreams(10000),
			grpc.InitialWindowSize(sqliteBenchGRPCWindowSize),
			grpc.InitialConnWindowSize(sqliteBenchGRPCWindowSize),
			grpc.WriteBufferSize(sqliteBenchGRPCWriteBufferSize),
			grpc.ReadBufferSize(sqliteBenchGRPCReadBufferSize),
		)
	}
	if authMode == "token" {
		logger, err := logging.ZapLogger("warn")
		require.NoError(b, err)
		hash, err := common.HashPassword("changeit")
		require.NoError(b, err)
		user := orisun.User{
			Id:             "bench-user",
			Name:           "Benchmark User",
			Username:       "admin",
			HashedPassword: hash,
			Roles:          []orisun.Role{orisun.RoleAdmin, orisun.RoleOperations},
		}
		authenticator := admin.NewAuthenticator(
			eventStore.getEvents,
			logger,
			benchBoundary,
			func(username string) (orisun.User, error) {
				if username != user.Username {
					return orisun.User{}, errors.New("user not found")
				}
				return user, nil
			},
		)
		opts = append(opts, grpc.ChainUnaryInterceptor(admin.UnaryAuthInterceptor(authenticator, logger)))
	}
	return opts
}

func startTCPEventStoreServer(
	b *testing.B,
	eventStore *benchmarkEventStoreServer,
	optionSet, authMode string,
) (addr string, stop func()) {
	b.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	grpcServer := grpc.NewServer(sqliteGRPCServerOptions(b, optionSet, authMode, eventStore)...)
	grpcapi.RegisterEventStoreServer(grpcServer, eventStore.EventStoreServer)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		grpcServer.Stop()
		err := <-serveErr
		if err != nil {
			require.ErrorIs(b, err, grpc.ErrServerStopped)
		}
	}
}

func authContextBasic() context.Context {
	creds := base64.StdEncoding.EncodeToString([]byte("admin:changeit"))
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Basic "+creds)
}

func staticTokenContextForClient(b *testing.B, client grpcapi.EventStoreClient) context.Context {
	b.Helper()
	var responseMD metadata.MD
	_, err := client.Ping(authContextBasic(), &grpcapi.PingRequest{}, grpc.Header(&responseMD))
	require.NoError(b, err)
	tokens := responseMD.Get("x-auth-token")
	require.NotEmpty(b, tokens)
	return metadata.AppendToOutgoingContext(context.Background(), "x-auth-token", tokens[0])
}

func createTCPBenchClients(b *testing.B, addr string, n int, authMode string) []sqliteGRPCBenchClient {
	b.Helper()
	clients := make([]sqliteGRPCBenchClient, 0, n)
	for range n {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(sqliteBenchGRPCMaxMessageSize)),
			grpc.WithInitialWindowSize(sqliteBenchGRPCWindowSize),
			grpc.WithInitialConnWindowSize(sqliteBenchGRPCWindowSize),
			grpc.WithWriteBufferSize(sqliteBenchGRPCWriteBufferSize),
			grpc.WithReadBufferSize(sqliteBenchGRPCReadBufferSize),
		)
		require.NoError(b, err)
		client := grpcapi.NewEventStoreClient(conn)
		ctx := context.Background()
		if authMode == "token" {
			ctx = staticTokenContextForClient(b, client)
		}
		clients = append(clients, sqliteGRPCBenchClient{conn: conn, client: client, ctx: ctx})
	}
	return clients
}

func closeTCPBenchClients(clients []sqliteGRPCBenchClient) {
	for _, client := range clients {
		_ = client.conn.Close()
	}
}

func runGRPCSaveBurst10000(b *testing.B, clients []sqliteGRPCBenchClient) {
	b.Helper()
	const burst = 10000

	events := make([]*grpcapi.EventToSave, burst)
	for j := range burst {
		id, _ := uuid.NewV7()
		events[j] = &grpcapi.EventToSave{
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
	for j := range burst {
		ev := events[j]
		client := clients[j%len(clients)]
		go func() {
			defer wg.Done()
			<-startCh
			if _, err := client.client.SaveEvents(client.ctx, &grpcapi.SaveEventsRequest{
				Boundary: benchBoundary,
				Events:   []*grpcapi.EventToSave{ev},
			}); err != nil {
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
	b.StopTimer()

	elapsed := time.Since(startTime)
	if fail > 0 {
		b.Logf("burst had %d failures", fail)
	}
	b.ReportMetric(float64(ok)/elapsed.Seconds(), "events/sec")
	b.ReportMetric(float64(elapsed.Milliseconds()), "ms/burst")
}

func BenchmarkSqlite_GRPCTransportBurst10000(b *testing.B) {
	for _, optionSet := range []string{"minimal", "cmd_like"} {
		for _, authMode := range []string{"none", "token"} {
			b.Run(fmt.Sprintf("transport=tcp/options=%s/auth=%s/client_conns=1", optionSet, authMode), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					saver, getter, _, teardown := setupBenchmarkPools(b)
					eventStore := newBenchmarkEventStoreServer(b, saver, getter)
					addr, stopServer := startTCPEventStoreServer(b, eventStore, optionSet, authMode)
					clients := createTCPBenchClients(b, addr, 1, authMode)

					runGRPCSaveBurst10000(b, clients)

					closeTCPBenchClients(clients)
					stopServer()
					teardown()
				}
			})
		}
	}
}

func setupBenchmarkPoolsWithMetadata(b *testing.B) (*SqliteSaveEvents, *SqliteGetEvents, map[string]*BoundaryPools, func()) {
	b.Helper()
	dir := b.TempDir()
	logger, err := logging.ZapLogger("warn")
	require.NoError(b, err)

	bp, err := OpenBoundaryPoolsWithConfig(context.Background(), config.SqliteConfig{
		Dir:         dir,
		Synchronous: "FULL",
	}, benchBoundary, benchBoundary)
	require.NoError(b, err)
	metadataPool, err := OpenMetadataPoolsWithConfig(context.Background(), config.SqliteConfig{
		Dir:         dir,
		Synchronous: "FULL",
	}, benchBoundary)
	require.NoError(b, err)
	pools := map[string]*BoundaryPools{benchBoundary: bp}
	metadataPools := map[string]*BoundaryPools{benchBoundary: metadataPool}

	saver := NewSqliteSaveEvents(pools, logger)
	getter := NewSqliteGetEvents(pools, logger)

	return saver, getter, metadataPools, func() {
		saver.close()
		_ = metadataPool.Close()
		_ = bp.Close()
	}
}

func startBenchmarkPollingPublisher(
	b *testing.B,
	ctx context.Context,
	getter *SqliteGetEvents,
	tracker orisun.EventPublishingTracker,
	signalProvider func(string) orisun.EventSignal,
) {
	b.Helper()
	logger, err := logging.ZapLogger("warn")
	require.NoError(b, err)
	cfg := config.AppConfig{}
	cfg.PollingPublisher.BatchSize = 1000
	manager := orisun.StartEventPolling(
		ctx,
		cfg,
		benchNoopLockProvider{},
		getter,
		benchFakeJetStream{},
		tracker,
		signalProvider,
		logger,
	)
	require.NoError(b, manager.StartBoundary(benchBoundary))
}

func BenchmarkSqlite_GRPCTransportWithPublisherBurst10000(b *testing.B) {
	for _, trackerMode := range []string{"none", "metadata"} {
		for _, authMode := range []string{"none", "token"} {
			for _, wakeDelay := range []time.Duration{0, 5 * time.Millisecond} {
				b.Run(fmt.Sprintf("transport=tcp/options=cmd_like/auth=%s/client_conns=1/publisher=fake_js/tracker=%s/wake_delay=%s", authMode, trackerMode, wakeDelay), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						saver, getter, metadataPools, teardown := setupBenchmarkPoolsWithMetadata(b)
						notifier := NewSqliteEventNotifierWithWakeDelay(time.Second, wakeDelay)
						saver.notifier = notifier
						var tracker orisun.EventPublishingTracker = benchNoopPublishingTracker{}
						if trackerMode == "metadata" {
							logger, err := logging.ZapLogger("warn")
							require.NoError(b, err)
							tracker = NewSqliteEventPublishingWithMetadata(metadataPools, logger)
						}
						pubCtx, cancelPublisher := context.WithCancel(context.Background())
						startBenchmarkPollingPublisher(b, pubCtx, getter, tracker, notifier.Signal)
						eventStore := newBenchmarkEventStoreServer(b, saver, getter)
						addr, stopServer := startTCPEventStoreServer(b, eventStore, "cmd_like", authMode)
						clients := createTCPBenchClients(b, addr, 1, authMode)

						runGRPCSaveBurst10000(b, clients)

						closeTCPBenchClients(clients)
						stopServer()
						cancelPublisher()
						teardown()
					}
				})
			}
		}
	}
}

func parseTxID(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// prepopulateStreams creates events for `numStreams` streams with `eventsPerStream` each,
// returning the stream IDs and final positions per stream.
func prepopulateStreams(
	b *testing.B,
	ctx context.Context,
	saver *SqliteSaveEvents,
	numStreams, eventsPerStream int,
) ([]string, []*orisun.Position) {
	streamIds := make([]string, numStreams)
	positions := make([]*orisun.Position, numStreams)

	for i := 0; i < numStreams; i++ {
		id, err := uuid.NewV7()
		require.NoError(b, err)
		streamIds[i] = id.String()
	}

	for sIdx := 0; sIdx < numStreams; sIdx++ {
		streamID := streamIds[sIdx]
		pos := orisun.NotExistsPosition()

		for e := 0; e < eventsPerStream; e++ {
			eventID, err := uuid.NewV7()
			require.NoError(b, err)

			data := fmt.Sprintf(`{"stream_id":"%s","eventType":"OrderPlaced","sequence":%d}`, streamID, e)
			meta := fmt.Sprintf(`{"timestamp":"%s"}`, time.Now().Format(time.RFC3339))

			tranID, gid, err := saver.Save(ctx, []orisun.EventWithMapTags{{
				EventId:   eventID.String(),
				EventType: "OrderPlaced",
				Data:      data,
				Metadata:  meta,
			}}, benchBoundary, &pos, nil)
			require.NoError(b, err, "prepopulate stream=%d event=%d", sIdx, e)

			pos = orisun.Position{
				CommitPosition:  parseTxID(tranID),
				PreparePosition: gid,
			}
		}

		positions[sIdx] = &orisun.Position{
			CommitPosition:  pos.CommitPosition,
			PreparePosition: pos.PreparePosition,
		}
	}

	return streamIds, positions
}

// BenchmarkSqlite_ConsistencyCheck_NoIndex measures Save throughput with a CCC criterion
// against an unindexed table. Mirrors postgres/postgres_benchmark_test.go for direct comparison.
func BenchmarkSqlite_ConsistencyCheck_NoIndex(b *testing.B) {
	saver, _, _, teardown := setupBenchmarkPools(b)
	defer teardown()

	ctx := context.Background()
	streamIds, positions := prepopulateStreams(b, ctx, saver, benchNumStreams, benchEventsPerStream)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sIdx := i % benchNumStreams
		streamID := streamIds[sIdx]

		eventID, err := uuid.NewV7()
		require.NoError(b, err)

		data := fmt.Sprintf(`{"stream_id":"%s","eventType":"OrderPlaced","sequence":%d}`, streamID, benchEventsPerStream+i)
		meta := fmt.Sprintf(`{"timestamp":"%s"}`, time.Now().Format(time.RFC3339))

		query := &orisun.Query{
			Criteria: []*orisun.Criterion{{
				Tags: []*orisun.Tag{{Key: "stream_id", Value: streamID}},
			}},
		}

		pos := positions[sIdx]
		tranID, gid, err := saver.Save(ctx, []orisun.EventWithMapTags{{
			EventId:   eventID.String(),
			EventType: "OrderPlaced",
			Data:      data,
			Metadata:  meta,
		}}, benchBoundary, pos, query)
		require.NoError(b, err, "iteration %d", i)

		newPos := orisun.Position{CommitPosition: parseTxID(tranID), PreparePosition: gid}
		positions[sIdx] = &newPos
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "saves/sec")
}

// BenchmarkSqlite_ConsistencyCheck_WithIndex creates a JSON-expression partial index
// on stream_id before measuring, isolating the index-vs-scan delta on the CCC path.
func BenchmarkSqlite_ConsistencyCheck_WithIndex(b *testing.B) {
	saver, _, admin, teardown := setupBenchmarkPools(b)
	defer teardown()

	ctx := context.Background()
	streamIds, positions := prepopulateStreams(b, ctx, saver, benchNumStreams, benchEventsPerStream)

	require.NoError(b, admin.CreateBoundaryIndex(ctx, benchBoundary, "stream_id",
		[]common.IndexField{{JsonKey: "stream_id", ValueType: "text"}}, nil, ""))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sIdx := i % benchNumStreams
		streamID := streamIds[sIdx]

		eventID, err := uuid.NewV7()
		require.NoError(b, err)

		data := fmt.Sprintf(`{"stream_id":"%s","eventType":"OrderPlaced","sequence":%d}`, streamID, benchEventsPerStream+i)
		meta := fmt.Sprintf(`{"timestamp":"%s"}`, time.Now().Format(time.RFC3339))

		query := &orisun.Query{
			Criteria: []*orisun.Criterion{{
				Tags: []*orisun.Tag{{Key: "stream_id", Value: streamID}},
			}},
		}

		pos := positions[sIdx]
		tranID, gid, err := saver.Save(ctx, []orisun.EventWithMapTags{{
			EventId:   eventID.String(),
			EventType: "OrderPlaced",
			Data:      data,
			Metadata:  meta,
		}}, benchBoundary, pos, query)
		require.NoError(b, err, "iteration %d", i)

		positions[sIdx] = &orisun.Position{CommitPosition: parseTxID(tranID), PreparePosition: gid}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "saves/sec")
}

// BenchmarkSqlite_SerialSave_NoCriteria measures the bare insert path: no CCC,
// single-event batches, serial.
func BenchmarkSqlite_SerialSave_NoCriteria(b *testing.B) {
	saver, _, _, teardown := setupBenchmarkPools(b)
	defer teardown()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eventID, _ := uuid.NewV7()
		_, _, err := saver.Save(ctx, []orisun.EventWithMapTags{{
			EventId:   eventID.String(),
			EventType: "Bench",
			Data:      `{"k":"v"}`,
			Metadata:  `{}`,
		}}, benchBoundary, nil, nil)
		require.NoError(b, err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "saves/sec")
}

// BenchmarkSqlite_BatchSave varies events-per-call to show amortization across batch sizes.
func BenchmarkSqlite_BatchSave(b *testing.B) {
	for _, batchSize := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			saver, _, _, teardown := setupBenchmarkPools(b)
			defer teardown()

			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				events := make([]orisun.EventWithMapTags, batchSize)
				for j := 0; j < batchSize; j++ {
					id, _ := uuid.NewV7()
					events[j] = orisun.EventWithMapTags{
						EventId:   id.String(),
						EventType: "Bench",
						Data:      `{"k":"v"}`,
						Metadata:  `{}`,
					}
				}
				_, _, err := saver.Save(ctx, events, benchBoundary, nil, nil)
				require.NoError(b, err)
			}
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/sec")
		})
	}
}

// BenchmarkSqlite_ConcurrentSave saturates the write pool with N goroutines.
// Reports events/sec — the metric to watch for v2 writer-coalescing wins.
func BenchmarkSqlite_ConcurrentSave(b *testing.B) {
	for _, conc := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers=%d", conc), func(b *testing.B) {
			saver, _, _, teardown := setupBenchmarkPools(b)
			defer teardown()

			ctx := context.Background()
			b.ResetTimer()

			var done int64
			var wg sync.WaitGroup
			startCh := make(chan struct{})
			start := time.Now()

			for w := 0; w < conc; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-startCh
					for {
						i := atomic.AddInt64(&done, 1)
						if i > int64(b.N) {
							return
						}
						id, _ := uuid.NewV7()
						_, _, err := saver.Save(ctx, []orisun.EventWithMapTags{{
							EventId:   id.String(),
							EventType: "Bench",
							Data:      `{"k":"v"}`,
							Metadata:  `{}`,
						}}, benchBoundary, nil, nil)
						if err != nil {
							b.Errorf("save failed: %v", err)
							return
						}
					}
				}()
			}
			close(startCh)
			wg.Wait()
			elapsed := time.Since(start)

			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/sec")
		})
	}
}

// BenchmarkSqlite_Burst5000 fires 5000 goroutines simultaneously, each issuing exactly
// one Save. Measures the queue-up + drain pattern at the boundary's write pool — the
// realistic worst-case shape for v1 (sequential through write pool size 1).
//
// Reports total wall time and events/sec across the burst. b.N controls how many bursts
// (`-benchtime=Nx` to run a fixed number).
func BenchmarkSqlite_Burst10000(b *testing.B) {
	const burst = 10000

	for _, synchronous := range []string{"FULL", "NORMAL"} {
		b.Run("sync="+synchronous, func(b *testing.B) {
			var totalOK int64
			var totalElapsed time.Duration

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Fresh DB per burst — avoids cross-iteration contamination on b.N>1.
				saver, _, _, teardown := setupBenchmarkPoolsWithSynchronous(b, synchronous)

				// Pre-build events so allocation isn't in the hot path.
				events := make([]orisun.EventWithMapTags, burst)
				for j := 0; j < burst; j++ {
					id, _ := uuid.NewV7()
					events[j] = orisun.EventWithMapTags{
						EventId:   id.String(),
						EventType: "BurstEvent",
						Data:      `{"k":"v"}`,
						Metadata:  `{}`,
					}
				}

				ctx := context.Background()
				var wg sync.WaitGroup
				var ok, fail int64
				startCh := make(chan struct{})
				wg.Add(burst)
				for j := 0; j < burst; j++ {
					ev := events[j]
					go func() {
						defer wg.Done()
						<-startCh
						if _, _, err := saver.Save(ctx, []orisun.EventWithMapTags{ev}, benchBoundary, nil, nil); err != nil {
							atomic.AddInt64(&fail, 1)
							return
						}
						atomic.AddInt64(&ok, 1)
					}()
				}

				b.StartTimer()
				startTime := time.Now()
				close(startCh)
				wg.Wait()
				elapsed := time.Since(startTime)
				b.StopTimer()

				if fail > 0 {
					b.Logf("burst had %d failures", fail)
				}
				totalOK += ok
				totalElapsed += elapsed

				teardown()
			}

			b.ReportMetric(float64(totalOK)/totalElapsed.Seconds(), "events/sec")
			b.ReportMetric(float64(totalElapsed.Milliseconds())/float64(b.N), "ms/burst")
		})
	}
}

// BenchmarkSqlite_GetEvents measures point-in-time read throughput with
// criteria filter, including protobuf materialization at the API boundary.
func BenchmarkSqlite_GetEvents(b *testing.B) {
	saver, getter, _, teardown := setupBenchmarkPools(b)
	defer teardown()

	ctx := context.Background()
	streamIds, _ := prepopulateStreams(b, ctx, saver, 100, 20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		streamID := streamIds[i%len(streamIds)]
		batch, err := getter.GetBatch(ctx, &orisun.GetEventsRequest{
			Boundary:  benchBoundary,
			Direction: orisun.Direction_ASC,
			Count:     50,
			Query: &orisun.Query{
				Criteria: []*orisun.Criterion{{
					Tags: []*orisun.Tag{{Key: "stream_id", Value: streamID}},
				}},
			},
		})
		require.NoError(b, err)
		_ = batch.Response()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "gets/sec")
}

// BenchmarkSqlite_GetEventsPacked measures the backend-to-internal-consumer
// path without protobuf materialization (for example, the publisher).
func BenchmarkSqlite_GetEventsPacked(b *testing.B) {
	saver, getter, _, teardown := setupBenchmarkPools(b)
	defer teardown()

	ctx := context.Background()
	streamIds, _ := prepopulateStreams(b, ctx, saver, 100, 20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		streamID := streamIds[i%len(streamIds)]
		_, err := getter.GetBatch(ctx, &orisun.GetEventsRequest{
			Boundary:  benchBoundary,
			Direction: orisun.Direction_ASC,
			Count:     50,
			Query: &orisun.Query{
				Criteria: []*orisun.Criterion{{
					Tags: []*orisun.Tag{{Key: "stream_id", Value: streamID}},
				}},
			},
		})
		require.NoError(b, err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "gets/sec")
}
