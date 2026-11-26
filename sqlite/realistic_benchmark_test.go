package sqlite

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	"github.com/OrisunLabs/Orisun/orisun"
)

// Realistic payloads: ~700 B of data and ~230 B of metadata per event, with
// nested objects and arrays, plus two managed indexes that every write maintains.
// The {"k":"v"} benchmarks isolate engine overhead; these approximate production.

var realisticEventTypes = []string{"OrderPlaced", "PaymentCaptured", "OrderShipped", "OrderDelivered"}

func realisticEvent(customer string, seq int) (eventType, data, metadata string) {
	eventType = realisticEventTypes[seq%len(realisticEventTypes)]
	id, _ := uuid.NewV7()
	data = fmt.Sprintf(`{"customerId":"%s","orderId":"ord-%s","sequence":%d,"status":"%s",`+
		`"currency":"EUR","totalAmount":%d.%02d,"taxAmount":%d.%02d,`+
		`"items":[{"sku":"SKU-%05d","name":"Ergonomic office chair, graphite","quantity":%d,"unitPrice":249.00},`+
		`{"sku":"SKU-%05d","name":"Adjustable monitor arm","quantity":1,"unitPrice":89.50},`+
		`{"sku":"SKU-%05d","name":"USB-C docking station","quantity":1,"unitPrice":159.99}],`+
		`"shippingAddress":{"line1":"%d Kastanienallee","line2":"Hinterhaus, 3. OG","city":"Berlin","postalCode":"10435","country":"DE"},`+
		`"channel":"web","promotionCodes":["AUTUMN24","FREESHIP"],"notes":"Leave with the concierge if nobody answers the door."}`,
		customer, id.String(), seq, eventType,
		400+seq%600, seq%100, 70+seq%100, seq%100,
		seq%99999, 1+seq%3, (seq+7)%99999, (seq+13)%99999, 1+seq%250)
	metadata = fmt.Sprintf(`{"correlationId":"%s","causationId":"%s","userId":"user-%d","tenantId":"tenant-eu-1",`+
		`"source":"checkout-service","schemaVersion":3,"traceparent":"00-%032x-%016x-01"}`,
		id.String(), customer, seq%9973, seq, seq)
	return eventType, data, metadata
}

func setupRealisticBenchmark(b *testing.B) (*SqliteSaveEvents, func()) {
	saver, _, admin, teardown := setupBenchmarkPools(b)
	ctx := context.Background()
	require.NoError(b, admin.CreateBoundaryIndex(ctx, benchBoundary, "customer",
		[]common.IndexField{{JsonKey: "customerId", ValueType: "text"}}, nil, ""))
	require.NoError(b, admin.CreateBoundaryIndex(ctx, benchBoundary, "shipped_orders",
		[]common.IndexField{{JsonKey: "orderId", ValueType: "text"}},
		[]common.IndexCondition{{Key: "__eventType", Operator: "=", Value: "OrderShipped"}}, ""))
	return saver, teardown
}

func BenchmarkSqlite_RealisticSave(b *testing.B) {
	for _, workers := range []int{1, 16, 100, 1024} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			saver, teardown := setupRealisticBenchmark(b)
			defer teardown()
			ctx := context.Background()
			var done atomic.Int64
			var wg sync.WaitGroup
			startCh := make(chan struct{})
			b.ResetTimer()
			start := time.Now()
			for w := range workers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					customer := fmt.Sprintf("cust-%d", w)
					<-startCh
					for {
						i := done.Add(1)
						if i > int64(b.N) {
							return
						}
						eventType, data, metadata := realisticEvent(customer, int(i))
						id, _ := uuid.NewV7()
						if _, _, err := saver.Save(ctx, []orisun.EventWithMapTags{{
							EventId: id.String(), EventType: eventType, Data: data, Metadata: metadata,
						}}, benchBoundary, nil); err != nil {
							b.Errorf("save failed: %v", err)
							return
						}
					}
				}(w)
			}
			close(startCh)
			wg.Wait()
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "events/sec")
		})
	}
}

func BenchmarkSqlite_RealisticBatchSave(b *testing.B) {
	const batchSize = 100
	saver, teardown := setupRealisticBenchmark(b)
	defer teardown()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := make([]orisun.EventWithMapTags, batchSize)
		for j := range events {
			eventType, data, metadata := realisticEvent("cust-batch", i*batchSize+j)
			id, _ := uuid.NewV7()
			events[j] = orisun.EventWithMapTags{EventId: id.String(), EventType: eventType, Data: data, Metadata: metadata}
		}
		_, _, err := saver.Save(ctx, events, benchBoundary, nil)
		require.NoError(b, err)
	}
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/sec")
}

// Each worker owns one customer with 20 events of history and saves under a
// CCC observation of that customer's context, carrying its latest position.
func BenchmarkSqlite_RealisticCCCSave(b *testing.B) {
	const history = 20
	for _, workers := range []int{16, 100} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			saver, teardown := setupRealisticBenchmark(b)
			defer teardown()
			ctx := context.Background()
			customers := make([]string, workers)
			positions := make([]*orisun.Position, workers)
			for w := range customers {
				customers[w] = fmt.Sprintf("cust-%d", w)
				events := make([]orisun.EventWithMapTags, history)
				for j := range events {
					eventType, data, metadata := realisticEvent(customers[w], j)
					id, _ := uuid.NewV7()
					events[j] = orisun.EventWithMapTags{EventId: id.String(), EventType: eventType, Data: data, Metadata: metadata}
				}
				tx, gid, err := saver.Save(ctx, events, benchBoundary, nil)
				require.NoError(b, err)
				positions[w] = &orisun.Position{CommitPosition: parseTxID(tx), PreparePosition: gid}
			}
			perWorker := max(b.N/workers, 1)
			var wg sync.WaitGroup
			startCh := make(chan struct{})
			b.ResetTimer()
			start := time.Now()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					query := &orisun.Query{Criteria: []*orisun.Criterion{{
						Tags: []*orisun.Tag{{Key: "customerId", Value: customers[w]}},
					}}}
					pos := positions[w]
					<-startCh
					for i := range perWorker {
						eventType, data, metadata := realisticEvent(customers[w], history+i)
						id, _ := uuid.NewV7()
						tx, gid, err := saver.Save(ctx, []orisun.EventWithMapTags{{
							EventId: id.String(), EventType: eventType, Data: data, Metadata: metadata,
						}}, benchBoundary, []*orisun.ConsistencyObservation{{Query: query, Position: pos}})
						if err != nil {
							b.Errorf("save failed: %v", err)
							return
						}
						pos = &orisun.Position{CommitPosition: parseTxID(tx), PreparePosition: gid}
					}
				}(w)
			}
			close(startCh)
			wg.Wait()
			b.ReportMetric(float64(perWorker*workers)/time.Since(start).Seconds(), "events/sec")
		})
	}
}
