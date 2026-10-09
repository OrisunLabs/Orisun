//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// These benchmarks prove the redesign's scaling claim: per-boundary writes are no
// longer serialised by a global sequence counter. Run against a live cluster:
//
//	ORISUN_FDB_TEST_CLUSTER_FILE=/etc/foundationdb/fdb.cluster \
//	  go test -tags foundationdb -run '^$' -bench BenchmarkFDB -benchtime=3s \
//	  -cpu 1,2,4,8 ./foundationdb/
//
// Reading the results with -cpu 1,2,4,8:
//   - AppendParallel and IndependentAggregates ns/op should DROP as -cpu rises
//     (throughput scales with parallelism) — no contended key on the write path.
//   - SingleHotAggregate ns/op should stay flat or worsen — all writers contend on
//     one aggregate's conflict range, which is the semantically required serial point.

func orderCriterion(orderID string) *eventstore.Query {
	return &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "order_id", Value: orderID}}},
	}}
}

func ensureOrderIDIndex(b *testing.B, backend *Backend) {
	b.Helper()
	if err := backend.CreateBoundaryIndex(context.Background(), "test", "order_id",
		[]eventstore.BoundaryIndexField{{JsonKey: "order_id", ValueType: "text"}},
		nil, eventstore.IndexCombinatorAND); err != nil {
		b.Fatalf("create order_id index: %v", err)
	}
}

// BenchmarkFDBAppendParallel: pure appends, no consistency condition. Zero
// contended keys — the cleanest scaling signal.
func BenchmarkFDBAppendParallel(b *testing.B) {
	backend := newTestBackend(b)
	ctx := context.Background()
	var counter int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			_, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
				EventId:   fmt.Sprintf("append-%d", n),
				EventType: "Appended",
				Data:      map[string]any{"k": "v"},
				Metadata:  map[string]any{},
			}}, "test", nil)
			if err != nil {
				b.Fatalf("append save: %v", err)
			}
		}
	})
}

// BenchmarkFDBIndependentAggregates: each goroutine owns a distinct aggregate and
// appends with a per-aggregate CCC condition + correct expected position. Conflict
// ranges are disjoint, so commits run in parallel and throughput scales.
func BenchmarkFDBIndependentAggregates(b *testing.B) {
	backend := newTestBackend(b)
	ctx := context.Background()
	ensureOrderIDIndex(b, backend)
	var aggCounter int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		agg := fmt.Sprintf("agg-%d", atomic.AddInt64(&aggCounter, 1))
		cond := orderCriterion(agg)
		// nil expected position means "context must be empty" — same as the
		// not-exists sentinel.
		var expected *eventstore.Position
		i := 0
		for pb.Next() {
			txID, gid, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
				EventId:   fmt.Sprintf("%s-%d", agg, i),
				EventType: "OrderEvent",
				Data:      map[string]any{"order_id": agg},
				Metadata:  map[string]any{},
			}}, "test", []*eventstore.ConsistencyObservation{{Query: cond, Position: expected}})
			if err != nil {
				b.Fatalf("independent save (sole writer should never conflict): %v", err)
			}
			commit, parseErr := strconv.ParseInt(txID, 10, 64)
			if parseErr != nil {
				b.Fatalf("parse txID %q: %v", txID, parseErr)
			}
			expected = &eventstore.Position{CommitPosition: commit, PreparePosition: gid}
			i++
		}
	})
}

// BenchmarkFDBSingleHotAggregate: every goroutine contends on one aggregate. Each
// write reads the current head, then saves with that expected position, retrying on
// the ALREADY_EXISTS another writer caused. This is the serial point — and it SHOULD
// be, because concurrent commands on the same aggregate cannot both win.
func BenchmarkFDBSingleHotAggregate(b *testing.B) {
	backend := newTestBackend(b)
	ctx := context.Background()
	ensureOrderIDIndex(b, backend)
	const agg = "hot"
	cond := orderCriterion(agg)
	var idCounter int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&idCounter, 1)
			for {
				resp, err := backend.GetBatch(ctx, &eventstore.GetEventsRequest{
					Boundary:  "test",
					Count:     1,
					Direction: eventstore.Direction_DESC,
					Query:     cond,
				})
				if err != nil {
					b.Fatalf("hot read head: %v", err)
				}
				var expected *eventstore.Position
				if len(resp) > 0 {
					expected = readEventPosition(resp[0])
				}
				_, _, err = backend.Save(ctx, []eventstore.EventWithMapTags{{
					EventId:   fmt.Sprintf("%s-%d", agg, id),
					EventType: "OrderEvent",
					Data:      map[string]any{"order_id": agg},
					Metadata:  map[string]any{},
				}}, "test", []*eventstore.ConsistencyObservation{{Query: cond, Position: expected}})
				if statuscode.CodeOf(err) == statuscode.AlreadyExists {
					continue // lost the race; re-read head and retry
				}
				if err != nil {
					b.Fatalf("hot save: %v", err)
				}
				break
			}
		}
	})
}

// BenchmarkFDBBurst10000 fires a fixed-size burst of concurrent single-event
// appends. Run with -benchtime=1x when you want wall-clock burst throughput:
//
//	ORISUN_FDB_TEST_CLUSTER_FILE=/etc/foundationdb/fdb.cluster \
//	  go test -tags foundationdb -run '^$' -bench BenchmarkFDBBurst10000 \
//	  -benchtime=1x ./foundationdb/
func BenchmarkFDBBurst10000(b *testing.B) {
	backend := newTestBackend(b)
	ctx := context.Background()
	const burstSize = 10000

	b.ResetTimer()
	totalOK := int64(0)
	for burst := 0; burst < b.N; burst++ {
		start := make(chan struct{})
		errs := make(chan error, burstSize)
		var wg sync.WaitGroup
		wg.Add(burstSize)
		for i := 0; i < burstSize; i++ {
			i := i
			go func() {
				defer wg.Done()
				<-start
				_, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
					EventId:   fmt.Sprintf("burst-%d-%d", burst, i),
					EventType: "BurstAppended",
					Data:      map[string]any{"burst": burst, "i": i},
					Metadata:  map[string]any{},
				}}, "test", nil)
				if err != nil {
					errs <- err
					return
				}
				atomic.AddInt64(&totalOK, 1)
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatalf("burst save: %v", err)
			}
		}
	}
	b.ReportMetric(float64(totalOK)/b.Elapsed().Seconds(), "events/sec")
}
