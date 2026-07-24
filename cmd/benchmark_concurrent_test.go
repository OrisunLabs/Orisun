//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This benchmark tests concurrent SaveEvents calls with batching enabled
// This simulates real-world production scenarios where multiple clients
// are sending events simultaneously

const (
	benchmarkPort   = "15007"
	grpcAddress     = "127.0.0.1:" + benchmarkPort
	workerCount     = 50  // Number of concurrent workers
	eventsPerWorker = 100 // Events each worker sends
	testBoundary    = "concurrent_bench_test"
	adminBoundary   = "concurrent_bench_admin"
)

type ConcurrentBenchmark struct {
	client    grpcapi.EventStoreClient
	authCtx   context.Context
	serverCmd *exec.Cmd
}

// BenchmarkConcurrentSaveEvents tests concurrent event saving with batching
func BenchmarkConcurrentSaveEvents(b *testing.B) {
	cb := &ConcurrentBenchmark{}

	// Start server with batching enabled
	b.Logf("Starting server with batching enabled...")
	if err := cb.startServer(b, true); err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	defer cb.stopServer()

	// Wait for server to be ready
	cb.waitForServer(b)

	// Setup client
	cb.setupClient(b)

	b.Log("Starting concurrent save benchmark with batching")
	b.ResetTimer()

	// Run with different numbers of concurrent workers
	concurrencyLevels := []int{10, 25, 50, 100}

	for _, workers := range concurrencyLevels {
		b.Run(fmt.Sprintf("Workers_%d", workers), func(b *testing.B) {
			var totalEvents int64
			var mu sync.Mutex

			for i := 0; i < b.N; i++ {
				// Create a context with timeout for this iteration
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Use a WaitGroup to coordinate workers
				var wg sync.WaitGroup
				startSignal := make(chan struct{})

				// Spawn workers
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()

						// Wait for start signal
						<-startSignal

						// Send events
						for e := 0; e < eventsPerWorker; e++ {
							event := cb.createTestEvent(workerID, e)

							_, err := cb.client.SaveEvents(
								cb.authCtx,
								&grpcapi.SaveEventsRequest{
									Events:   []*grpcapi.EventToSave{event},
									Boundary: testBoundary,
								},
							)

							if err != nil {
								b.Logf("Worker %d: Failed to save event %d: %v", workerID, e, err)
								return
							}

							mu.Lock()
							totalEvents++
							mu.Unlock()
						}
					}(w)
				}

				// Start all workers simultaneously
				startTime := time.Now()
				close(startSignal)

				// Wait for all workers to complete
				wg.Wait()
				duration := time.Since(startTime)

				// Calculate throughput
				eventsPerSec := float64(totalEvents) / duration.Seconds()
				b.ReportMetric(eventsPerSec, "events/sec")
				b.Logf("Workers: %d, Events: %d, Duration: %v, Throughput: %.2f events/sec",
					workers, totalEvents, duration, eventsPerSec)
			}
		})
	}
}

// BenchmarkConcurrentWithBatching compares batching vs no batching
func BenchmarkConcurrentWithBatching(b *testing.B) {
	testCases := []struct {
		name            string
		batching        bool
		workers         int
		eventsPerWorker int
	}{
		{
			name:            "NoBatching_10Workers",
			batching:        false,
			workers:         10,
			eventsPerWorker: 100,
		},
		{
			name:            "WithBatching_10Workers",
			batching:        true,
			workers:         10,
			eventsPerWorker: 100,
		},
		{
			name:            "NoBatching_50Workers",
			batching:        false,
			workers:         50,
			eventsPerWorker: 100,
		},
		{
			name:            "WithBatching_50Workers",
			batching:        true,
			workers:         50,
			eventsPerWorker: 100,
		},
		{
			name:            "NoBatching_100Workers",
			batching:        false,
			workers:         100,
			eventsPerWorker: 50,
		},
		{
			name:            "WithBatching_100Workers",
			batching:        true,
			workers:         100,
			eventsPerWorker: 50,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			cb := &ConcurrentBenchmark{}

			b.Logf("Starting server (batching=%v)...", tc.batching)
			if err := cb.startServer(b, tc.batching); err != nil {
				b.Fatalf("Failed to start server: %v", err)
			}
			defer cb.stopServer()

			cb.waitForServer(b)
			cb.setupClient(b)

			b.Logf("Running test: %s", tc.name)

			var totalEvents int64
			var mu sync.Mutex

			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()

				var wg sync.WaitGroup
				startSignal := make(chan struct{})

				for w := 0; w < tc.workers; w++ {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()
						<-startSignal

						for e := 0; e < tc.eventsPerWorker; e++ {
							event := cb.createTestEvent(workerID, e)

							_, err := cb.client.SaveEvents(
								cb.authCtx,
								&grpcapi.SaveEventsRequest{
									Events:   []*grpcapi.EventToSave{event},
									Boundary: testBoundary,
								},
							)

							if err != nil {
								return
							}

							mu.Lock()
							totalEvents++
							mu.Unlock()
						}
					}(w)
				}

				startTime := time.Now()
				close(startSignal)
				wg.Wait()
				duration := time.Since(startTime)

				eventsPerSec := float64(totalEvents) / duration.Seconds()
				b.ReportMetric(eventsPerSec, "events/sec")

				b.Logf("%s: Workers=%d, Events=%d, Duration=%v, Throughput=%.2f events/sec",
					tc.name, tc.workers, totalEvents, duration, eventsPerSec)
			}
		})
	}
}

func (cb *ConcurrentBenchmark) startServer(b *testing.B, enableBatching bool) error {
	// Set environment variables
	os.Setenv("ORISUN_PG_HOST", "localhost")
	os.Setenv("ORISUN_PG_PORT", "5432")
	os.Setenv("ORISUN_PG_USER", "postgres")
	os.Setenv("ORISUN_PG_PASSWORD", "postgres")
	os.Setenv("ORISUN_PG_NAME", "orisun_test")
	os.Setenv("ORISUN_PG_ADMIN_SCHEMA", "admin")
	os.Setenv("ORISUN_ADMIN_BOUNDARY", adminBoundary)
	os.Setenv("ORISUN_NATS_PORT", "14225")
	os.Setenv("ORISUN_GRPC_PORT", benchmarkPort)

	// Enable/disable batching
	if enableBatching {
		os.Setenv("ORISUN_BATCH_ENABLED", "true")
		b.Log("Batching ENABLED for this test")
	} else {
		os.Setenv("ORISUN_BATCH_ENABLED", "false")
		b.Log("Batching DISABLED for this test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the server
	cmd := execCommand(ctx, "go", "run", "../basic_usage.go")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	cb.serverCmd = cmd

	// Wait for server to start
	time.Sleep(4 * time.Second)
	return nil
}

func (cb *ConcurrentBenchmark) stopServer() {
	if cb.serverCmd != nil && cb.serverCmd.Process != nil {
		cb.serverCmd.Process.Kill()
		cb.serverCmd.Wait()
	}
}

func (cb *ConcurrentBenchmark) waitForServer(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Try to connect with retry
	for i := 0; i < 60; i++ {
		select {
		case <-ctx.Done():
			b.Fatalf("Timeout waiting for server to start")
		case <-time.After(1 * time.Second):
			conn, err := grpc.DialContext(
				ctx,
				grpcAddress,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err == nil {
				conn.Close()
				b.Logf("Server ready after %d seconds", i+1)
				return
			}
		}
	}

	b.Fatalf("Failed to connect to server")
}

func (cb *ConcurrentBenchmark) setupClient(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		b.Fatalf("Failed to connect to gRPC server: %v", err)
	}

	cb.client = grpcapi.NewEventStoreClient(conn)

	// Setup authentication context - retry until admin user exists
	for i := 0; i < 30; i++ {
		pingResp, err := cb.client.Ping(context.Background(), &grpcapi.PingRequest{})
		if err == nil && pingResp != nil {
			b.Logf("Authentication successful on attempt %d", i+1)
			break
		}
		b.Logf("Authentication attempt %d failed: %v, retrying...", i+1, err)
		time.Sleep(time.Second)
	}

	// Create authenticated context
	adminUser := &orisun.User{
		Username: "admin",
		Roles:    []orisun.Role{orisun.RoleAdmin},
	}
	cb.authCtx = context.WithValue(context.Background(), orisun.UserContextKey, adminUser)
}

func (cb *ConcurrentBenchmark) createTestEvent(workerID, eventID int) *grpcapi.EventToSave {
	eventUUID, _ := uuid.NewV7()
	return &grpcapi.EventToSave{
		EventId:   eventUUID.String(),
		EventType: "ConcurrentTestEvent",
		Data:      fmt.Sprintf(`{"worker_id":%d,"event_id":%d,"timestamp":%d}`, workerID, eventID, time.Now().Unix()),
		Metadata:  fmt.Sprintf(`{"source":"worker_%d","test":"concurrent"}`, workerID),
	}
}

func execCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}
