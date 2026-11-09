package sqlite

import (
	"fmt"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Isolate prepared insertion CPU from document parsing and durable commit latency. Each iteration rolls back
// so every sample starts with the same history and event positions.
func BenchmarkSqliteInsertDocuments(b *testing.B) {
	for _, count := range []int{1, 8, 128} {
		for _, size := range []int{128, 4096} {
			b.Run(fmt.Sprintf("events=%d/payload=%d", count, size), func(b *testing.B) {
				saver, _, _, cleanup := setupBenchmarkPools(b)
				defer cleanup()
				pool, _ := saver.registry.eventPool(benchBoundary)
				conn, err := pool.Write.Take(b.Context())
				require.NoError(b, err)
				defer pool.Write.Put(conn)
				events := make(orisun.PreparedEventBatch, count)
				for i := range events {
					events[i] = orisun.PreparedEvent{EventId: fmt.Sprint(i), EventType: "Fact", DataJSON: fmt.Sprintf(`{"payload":%q}`, strings.Repeat("x", size)), MetadataJSON: "{}"}
				}
				prepared := prepareEventRows(events)
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					require.NoError(b, sqlitex.Execute(conn, "BEGIN IMMEDIATE", nil))
					require.NoError(b, insertWriteContext(conn, int64(count), "[]"))
					require.NoError(b, insertEventBatch(conn, prepared, 1, int64(count)))
					require.NoError(b, sqlitex.Execute(conn, "ROLLBACK", nil))
				}
			})
		}
	}
}
