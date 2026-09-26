package sqlite

import (
	"fmt"
	"strconv"
	"testing"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

// Exercise the production flush against indexed history, including an absent OR
// branch and a final event outside the context in multi-event requests.
func BenchmarkSqlite_GroupCommitIndexedHistory(b *testing.B) {
	for _, eventCount := range []int{1, 2} {
		b.Run(fmt.Sprintf("events=%d", eventCount), func(b *testing.B) {
			saver, _, admin, cleanup := setupBenchmarkPools(b)
			defer cleanup()
			ctx := b.Context()
			const contexts, history = 128, 64
			positions := make([]orisun.Position, contexts)
			for c := range contexts {
				seed := make(orisun.PreparedEventBatch, history)
				for i := range seed {
					seed[i] = orisun.PreparedEvent{EventId: fmt.Sprintf("seed-%d-%d", c, i), EventType: "Fact", DataJSON: fmt.Sprintf(`{"context":"%d"}`, c), MetadataJSON: "{}"}
				}
				tx, gid, err := saver.SavePrepared(ctx, seed, benchBoundary, nil)
				require.NoError(b, err)
				positions[c] = orisun.Position{CommitPosition: parseTxID(tx), PreparePosition: gid}
			}
			for _, key := range []string{"context", "reference"} {
				require.NoError(b, admin.CreateBoundaryIndex(ctx, benchBoundary, key, []common.IndexField{{JsonKey: key, ValueType: "text"}}, []common.IndexCondition{{Key: "__eventType", Operator: "=", Value: "Fact"}}, "AND"))
			}
			pool, _ := saver.registry.eventPool(benchBoundary)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				requests := make([]*sqliteSaveRequest, contexts)
				for c := range contexts {
					events := orisun.PreparedEventBatch{{EventId: fmt.Sprintf("event-%d-%d", n, c), EventType: "Fact", DataJSON: fmt.Sprintf(`{"context":"%d"}`, c), MetadataJSON: "{}"}}
					if eventCount == 2 {
						events = append(events, orisun.PreparedEvent{EventId: fmt.Sprintf("other-%d-%d", n, c), EventType: "Other", DataJSON: "{}", MetadataJSON: "{}"})
					}
					checks := []orisun.ConsistencyCheck{{Position: positions[c], Criteria: []orisun.ReadCriterion{
						{Tags: []orisun.ReadTag{{Key: "__eventType", Value: "Fact"}, {Key: "context", Value: strconv.Itoa(c)}}},
						{Tags: []orisun.ReadTag{{Key: "__eventType", Value: "Fact"}, {Key: "reference", Value: strconv.Itoa(c)}}},
					}}}
					encoded, err := orisun.MarshalConsistency(checks)
					require.NoError(b, err)
					requests[c] = &sqliteSaveRequest{ctx: ctx, inserts: events, consistency: checks, consistencyJSON: string(encoded), result: make(chan sqliteSaveResult, 1)}
				}
				saver.runFlush(benchBoundary, pool, requests)
				for c, req := range requests {
					result := <-req.result
					require.NoError(b, result.err)
					positions[c] = orisun.Position{CommitPosition: parseTxID(result.transactionID), PreparePosition: result.globalID - int64(eventCount-1)}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*contexts)/b.Elapsed().Seconds(), "saves/sec")
		})
	}
}
