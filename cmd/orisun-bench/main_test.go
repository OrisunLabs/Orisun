package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSelectScenarios(t *testing.T) {
	selected, err := selectScenarios("write-conditional-1,read-conditional")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 ||
		selected[0].Name != "write-conditional-1" ||
		selected[1].Name != "read-conditional" {
		t.Fatalf("unexpected scenarios: %#v", selected)
	}
	if _, err := selectScenarios("missing"); err == nil {
		t.Fatal("expected an unknown-scenario error")
	}
}

func TestParsePositiveInts(t *testing.T) {
	values, err := parsePositiveInts("1, 4,4,16")
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 4, 16}
	if len(values) != len(want) {
		t.Fatalf("got %v, want %v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("got %v, want %v", values, want)
		}
	}
	if _, err := parsePositiveInts("1,zero"); err == nil {
		t.Fatal("expected invalid concurrency to fail")
	}
}

func TestValidateConfig(t *testing.T) {
	config := validTestConfig()
	if err := validateConfig(config); err != nil {
		t.Fatal(err)
	}
	config.Duration = 0
	if err := validateConfig(config); err == nil {
		t.Fatal("expected a zero duration to fail")
	}
	config = validTestConfig()
	config.ReadBatch = 10_001
	if err := validateConfig(config); err == nil {
		t.Fatal("expected an oversized read batch to fail")
	}
}

func TestLabelsFlag(t *testing.T) {
	labels := labelsFlag{}
	if err := labels.Set("machine=MacBook Pro"); err != nil {
		t.Fatal(err)
	}
	if labels["machine"] != "MacBook Pro" {
		t.Fatalf("unexpected label: %q", labels["machine"])
	}
	if err := labels.Set("missing-value"); err == nil {
		t.Fatal("expected malformed label to fail")
	}
}

func TestValidateBuiltArtifacts(t *testing.T) {
	restore := setRunnerBuildInfo("local", "1234567", "2026-07-30T12:00:00Z", "false")
	defer restore()

	server := &grpcapi.GetServerInfoResponse{
		Version:   "local",
		GitCommit: "1234567",
		BuildTime: "2026-07-30T12:00:00Z",
	}
	if err := validateBuiltArtifacts(server); err != nil {
		t.Fatal(err)
	}
	server.Version = "dev"
	err := validateBuiltArtifacts(server)
	if err == nil || !strings.Contains(err.Error(), "do not use go run") {
		t.Fatalf("expected development server rejection, got %v", err)
	}
}

func TestValidateBuiltArtifactsRejectsDevelopmentRunner(t *testing.T) {
	restore := setRunnerBuildInfo("dev", "unknown", "unknown", "false")
	defer restore()

	err := validateBuiltArtifacts(&grpcapi.GetServerInfoResponse{
		Version:   "local",
		GitCommit: "1234567",
		BuildTime: "2026-07-30T12:00:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "benchmark runner") {
		t.Fatalf("expected development runner rejection, got %v", err)
	}
}

func TestReportWritesJSONAndCSV(t *testing.T) {
	restore := setRunnerBuildInfo("local", "1234567", "2026-07-30T12:00:00Z", "false")
	defer restore()

	config := validTestConfig()
	config.OutputDir = t.TempDir()
	config.Labels = labelsFlag{"machine": "test-host"}
	scenarios := []scenario{{
		Name:             "write-conditional-1",
		Kind:             "write-conditional",
		EventsPerRequest: 1,
	}}
	report, err := newReport(config, []int{4}, scenarios, &grpcapi.GetServerInfoResponse{
		Version:   "local",
		GitCommit: "1234567",
		BuildTime: "2026-07-30T12:00:00Z",
		Backend:   grpcapi.StorageBackend_STORAGE_BACKEND_POSTGRES,
	})
	if err != nil {
		t.Fatal(err)
	}
	report.Results = append(report.Results, benchmarkResult{
		Boundary:          config.Boundary,
		Scenario:          scenarios[0].Name,
		Kind:              scenarios[0].Kind,
		Concurrency:       4,
		EventsPerRequest:  1,
		Requests:          100,
		Events:            100,
		RequestsPerSecond: 200,
		EventsPerSecond:   200,
		LatencyP50NS:      int64(time.Millisecond),
		LatencyP99NS:      int64(2 * time.Millisecond),
	})
	report.CompletedAt = time.Now().UTC()
	if err := report.write(true); err != nil {
		t.Fatal(err)
	}

	jsonFile, err := os.Open(filepath.Join(report.runDir, "results.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer jsonFile.Close()
	var decoded benchmarkReport
	if err := json.NewDecoder(jsonFile).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Completed || len(decoded.Results) != 1 {
		t.Fatalf("unexpected JSON report: %#v", decoded)
	}
	if decoded.Labels["machine"] != "test-host" {
		t.Fatalf("labels were not preserved: %#v", decoded.Labels)
	}

	csvFile, err := os.Open(filepath.Join(report.runDir, "results.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer csvFile.Close()
	rows, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1][0] != scenarios[0].Name {
		t.Fatalf("unexpected CSV rows: %#v", rows)
	}
}

func TestCopyLabelsDoesNotAliasConfiguration(t *testing.T) {
	labels := labelsFlag{"machine": "before"}
	copied := copyLabels(labels)
	labels["machine"] = "after"
	if copied["machine"] != "before" {
		t.Fatalf("copy was mutated: %#v", copied)
	}
}

func TestMeasurementEndedTreatsWindowDeadlineAsCancellation(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if !measurementEnded(ctx, status.Error(codes.DeadlineExceeded, "done")) {
		t.Fatal("expected elapsed measurement deadline to end the worker")
	}
}

func TestBoundaryRuntimeStarting(t *testing.T) {
	if !boundaryRuntimeStarting(status.Error(codes.FailedPrecondition, "boundary is not active")) {
		t.Fatal("expected not-active boundary to be retryable")
	}
	if boundaryRuntimeStarting(status.Error(codes.PermissionDenied, "boundary is not active")) {
		t.Fatal("permission failure must not be retryable")
	}
}

func TestPercentile(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5}
	if got := percentile(values, 0.50); got != 3 {
		t.Fatalf("p50 = %d, want 3", got)
	}
	if got := percentile(values, 0.99); got != 5 {
		t.Fatalf("p99 = %d, want 5", got)
	}
}

func validTestConfig() benchmarkConfig {
	return benchmarkConfig{
		Address:             "127.0.0.1:5005",
		Boundary:            "benchmark",
		Backend:             "postgres",
		Namespace:           "benchmark",
		AuthMode:            "token",
		Duration:            time.Second,
		Warmup:              time.Second,
		Timeout:             time.Second,
		PrepopulateEvents:   1_000,
		TagCardinality:      100,
		PrepopulateBatch:    100,
		ReadBatch:           100,
		ConstrainedReadRate: 10_000,
		OppositeClients:     4,
		PayloadBytes:        256,
		OutputDir:           "benchmark-results",
		Labels:              labelsFlag{},
	}
}

func setRunnerBuildInfo(version, commit, buildTime, dirty string) func() {
	previousVersion := benchmarkVersion
	previousCommit := benchmarkGitCommit
	previousBuildTime := benchmarkBuildTime
	previousDirty := benchmarkDirty
	benchmarkVersion = version
	benchmarkGitCommit = commit
	benchmarkBuildTime = buildTime
	benchmarkDirty = dirty
	return func() {
		benchmarkVersion = previousVersion
		benchmarkGitCommit = previousCommit
		benchmarkBuildTime = previousBuildTime
		benchmarkDirty = previousDirty
	}
}
