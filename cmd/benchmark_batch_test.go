//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/OrisunLabs/Orisun/postgres"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This benchmark tests Approach 1: PostgreSQL-level batch function
// It directly calls SaveBatch to measure performance improvement

const (
	batchTimeout    = 10 * time.Millisecond
	maxBatchSize    = 100
	testNumEvents   = 1000
	testNumRequests = 100
)

type BatchBenchmark struct {
	grpcPort      string
	client        grpcapi.EventStoreClient
	authCtx       context.Context
	postgresSaver *postgres.PostgresSaveEvents
}

func (bb *BatchBenchmark) authContext() context.Context {
	if bb.authCtx == nil {
		bb.authCtx = context.Background()
	}
	return bb.authCtx
}

func BenchmarkSaveEvents_BatchApproach1(b *testing.B) {
	bb := &BatchBenchmark{}

	// Start Orisun server
	bb.startServer(b)
	defer bb.stopServer(b)

	// Connect to PostgreSQL directly for batch calls
	bb.connectPostgres(b)

	// Wait for server to be ready
	bb.waitForServer(b)

	b.Log("Starting BenchmarkSaveEvents_BatchApproach1")

	// Reset timer after setup
	b.ResetTimer()

	// Test different batch sizes
	batchSizes := []int{10, 50, 100}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			var totalEvents int64

			for i := 0; i < b.N; i++ {
				// Create batch requests
				requests := make([]postgres.BatchRequest, batchSize)
				for j := 0; j < batchSize; j++ {
					events := bb.createTestEvents(1)
					requests[j] = postgres.BatchRequest{
						Events:   events,
						Boundary: "benchmark_test",
						ExpectedPosition: &orisun.Position{
							CommitPosition:  -1,
							PreparePosition: -1,
						},
					}
				}

				// Execute batch
				results, err := bb.postgresSaver.SaveBatch(context.Background(), requests)
				if err != nil {
					b.Fatalf("SaveBatch failed: %v", err)
				}

				// Verify all succeeded
				for _, result := range results {
					if !result.Success {
						b.Fatalf("Batch request failed: %s", result.ErrorMessage)
					}
				}

				totalEvents += int64(batchSize)
			}

			// Report events per second
			b.ReportMetric(float64(totalEvents)/float64(b.Elapsed().Seconds()), "events/sec")
		})
	}
}

func BenchmarkSaveEvents_ConcurrentWithBatching(b *testing.B) {
	bb := &BatchBenchmark{}

	bb.startServer(b)
	defer bb.stopServer(b)
	bb.connectPostgres(b)
	bb.waitForServer(b)

	b.Log("Starting BenchmarkSaveEvents_ConcurrentWithBatching")

	b.ResetTimer()

	// Simulate concurrent requests being batched
	numWorkers := 10
	requestsPerWorker := 10
	batchSize := 50

	var totalEvents int64

	b.RunParallel(func(pb *testing.PB) {
		workerID := time.Now().UnixNano()

		for pb.Next() {
			// Collect requests from this worker
			requests := make([]postgres.BatchRequest, requestsPerWorker)
			for i := 0; i < requestsPerWorker; i++ {
				events := bb.createTestEvents(1)
				requests[i] = postgres.BatchRequest{
					Events:   events,
					Boundary: "benchmark_test",
					ExpectedPosition: &orisun.Position{
						CommitPosition:  -1,
						PreparePosition: -1,
					},
				}
			}

			// In a real implementation, these would be accumulated
			// across workers and flushed when timeout or size is reached
			// For now, just save directly to measure the batch function performance
			results, err := bb.postgresSaver.SaveBatch(context.Background(), requests)
			if err != nil {
				b.Fatalf("SaveBatch failed: %v", err)
			}

			for _, result := range results {
				if !result.Success {
					b.Fatalf("Batch request failed: %s", result.ErrorMessage)
				}
			}

			totalEvents += int64(requestsPerWorker)
		}

		_ = workerID
	})

	b.ReportMetric(float64(totalEvents)/float64(b.Elapsed().Seconds()), "events/sec")
}

func (bb *BatchBenchmark) startServer(b *testing.B) {
	os.Setenv("ORISUN_PG_HOST", "localhost")
	os.Setenv("ORISUN_PG_PORT", "5432")
	os.Setenv("ORISUN_PG_USER", "postgres")
	os.Setenv("ORISUN_PG_PASSWORD", "postgres")
	os.Setenv("ORISUN_PG_NAME", "orisun_test")
	os.Setenv("ORISUN_PG_ADMIN_SCHEMA", "admin")
	os.Setenv("ORISUN_ADMIN_BOUNDARY", "benchmark_admin")
	os.Setenv("ORISUN_NATS_PORT", "14224")
	os.Setenv("ORISUN_GRPC_PORT", "15006")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := execCommand(ctx, "go", "run", "../basic_usage.go")
	if err := cmd.Start(); err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}

	// Wait a bit for server to start
	time.Sleep(3 * time.Second)
	bb.grpcPort = "15006"
}

func (bb *BatchBenchmark) stopServer(b *testing.B) {
	// Server will be killed when context is cancelled
}

func (bb *BatchBenchmark) connectPostgres(b *testing.B) {
	// This is a placeholder - in a real test we'd get the PostgresSaveEvents
	// instance from the running server or create a new connection
	// For now, we'll skip this and document what needs to be done
	b.Log("Note: PostgreSQL connection setup needed for full test")
}

func (bb *BatchBenchmark) waitForServer(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		fmt.Sprintf("127.0.0.1:%s", bb.grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		b.Fatalf("Failed to connect to server: %v", err)
	}
	conn.Close()

	// Create client
	bb.client = grpcapi.NewEventStoreClient(conn)

	// Test authentication
	var pingResp *grpcapi.PingResponse
	for i := 0; i < 30; i++ {
		pingResp, err = bb.client.Ping(bb.authContext(), &grpcapi.PingRequest{})
		if err == nil {
			b.Logf("Authentication successful on attempt %d", i+1)
			break
		}
		b.Logf("Authentication attempt %d failed: %v, retrying...", i+1, err)
		time.Sleep(time.Second)
	}

	if pingResp == nil {
		b.Fatalf("Authentication failed after 30 attempts")
	}
}

func (bb *BatchBenchmark) createTestEvents(count int) []orisun.EventWithMapTags {
	events := make([]orisun.EventWithMapTags, count)
	for i := 0; i < count; i++ {
		eventID, _ := uuid.NewV7()
		events[i] = orisun.EventWithMapTags{
			EventId:   eventID.String(),
			EventType: "TestEvent",
			Data: map[string]any{
				"test": "data",
				"id":   i,
			},
			Metadata: map[string]any{
				"timestamp": time.Now().Unix(),
			},
		}
	}
	return events
}

// Helper function to execute commands
func execCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	return &exec.Cmd{
		Context: ctx,
		Path:    name,
		Args:    args,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

// Test to verify batch function works end-to-end
func TestBatchFunction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	bb := &BatchBenchmark{}

	bb.startServer(t)
	defer bb.stopServer(t)
	bb.connectPostgres(t)
	bb.waitForServer(t)

	// Create batch requests
	requests := make([]postgres.BatchRequest, 10)
	for i := 0; i < 10; i++ {
		events := bb.createTestEvents(5)
		requests[i] = postgres.BatchRequest{
			Events:   events,
			Boundary: "benchmark_test",
			ExpectedPosition: &orisun.Position{
				CommitPosition:  -1,
				PreparePosition: -1,
			},
		}
	}

	// Execute batch
	results, err := bb.postgresSaver.SaveBatch(context.Background(), requests)
	if err != nil {
		t.Fatalf("SaveBatch failed: %v", err)
	}

	// Verify results
	if len(results) != len(requests) {
		t.Fatalf("Expected %d results, got %d", len(requests), len(results))
	}

	for i, result := range results {
		if !result.Success {
			t.Errorf("Request %d failed: %s", i, result.ErrorMessage)
		}
		if result.TransactionID == "" {
			t.Errorf("Request %d has empty transaction ID", i)
		}
	}

	t.Log("Batch function test passed!")
}
