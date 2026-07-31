package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
)

type benchmarkReport struct {
	SchemaVersion int                     `json:"schema_version"`
	StartedAt     time.Time               `json:"started_at"`
	CompletedAt   time.Time               `json:"completed_at,omitempty"`
	Completed     bool                    `json:"completed"`
	Config        benchmarkConfigSnapshot `json:"config"`
	Runner        runnerSnapshot          `json:"runner"`
	Server        serverSnapshot          `json:"server"`
	Environment   map[string]string       `json:"environment"`
	Labels        map[string]string       `json:"labels,omitempty"`
	Results       []benchmarkResult       `json:"results"`
	runDir        string
}

type benchmarkConfigSnapshot struct {
	Address             string        `json:"address"`
	Boundary            string        `json:"boundary"`
	Backend             string        `json:"backend"`
	Namespace           string        `json:"namespace"`
	Scenarios           []string      `json:"scenarios"`
	Concurrency         []int         `json:"concurrency"`
	Duration            time.Duration `json:"duration_ns"`
	Warmup              time.Duration `json:"warmup_ns"`
	Timeout             time.Duration `json:"timeout_ns"`
	PrepopulateEvents   int           `json:"prepopulate_events"`
	TagCardinality      int           `json:"tag_cardinality"`
	PrepopulateBatch    int           `json:"prepopulate_batch"`
	ReadBatch           int           `json:"read_batch"`
	ConstrainedReadRate int           `json:"constrained_read_rate"`
	OppositeClients     int           `json:"opposite_clients"`
	PayloadBytes        int           `json:"payload_bytes"`
	CreateBoundary      bool          `json:"create_boundary"`
	CreateIndex         bool          `json:"create_index"`
	Smoke               bool          `json:"smoke"`
}

type runnerSnapshot struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildTime string `json:"build_time"`
	Dirty     bool   `json:"dirty"`
}

type serverSnapshot struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildTime string `json:"build_time"`
	Backend   string `json:"backend"`
}

func newReport(
	config benchmarkConfig,
	concurrency []int,
	scenarios []scenario,
	server *grpcapi.GetServerInfoResponse,
) (*benchmarkReport, error) {
	started := time.Now().UTC()
	runDir := filepath.Join(config.OutputDir, started.Format("20060102T150405Z"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, fmt.Errorf("create result directory: %w", err)
	}
	scenarioNames := make([]string, len(scenarios))
	for i, workload := range scenarios {
		scenarioNames[i] = workload.Name
	}
	report := &benchmarkReport{
		SchemaVersion: 1,
		StartedAt:     started,
		Config: benchmarkConfigSnapshot{
			Address:             config.Address,
			Boundary:            config.Boundary,
			Backend:             config.Backend,
			Namespace:           config.Namespace,
			Scenarios:           scenarioNames,
			Concurrency:         append([]int(nil), concurrency...),
			Duration:            config.Duration,
			Warmup:              config.Warmup,
			Timeout:             config.Timeout,
			PrepopulateEvents:   config.PrepopulateEvents,
			TagCardinality:      config.TagCardinality,
			PrepopulateBatch:    config.PrepopulateBatch,
			ReadBatch:           config.ReadBatch,
			ConstrainedReadRate: config.ConstrainedReadRate,
			OppositeClients:     config.OppositeClients,
			PayloadBytes:        config.PayloadBytes,
			CreateBoundary:      config.CreateBoundary,
			CreateIndex:         config.CreateIndex,
			Smoke:               config.Smoke,
		},
		Runner: runnerSnapshot{
			Version:   benchmarkVersion,
			GitCommit: benchmarkGitCommit,
			BuildTime: benchmarkBuildTime,
			Dirty:     benchmarkDirty == "true",
		},
		Environment: environmentSummary(),
		Labels:      copyLabels(config.Labels),
		Results:     make([]benchmarkResult, 0, len(scenarios)*len(concurrency)),
		runDir:      runDir,
	}
	if server != nil {
		report.Server = serverSnapshot{
			Version:   server.Version,
			GitCommit: server.GitCommit,
			BuildTime: server.BuildTime,
			Backend:   server.Backend.String(),
		}
	}
	return report, nil
}

func copyLabels(labels labelsFlag) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}

func (r *benchmarkReport) totalErrors() int64 {
	var total int64
	for _, result := range r.Results {
		total += result.Errors + result.BackgroundErrors
	}
	return total
}

func (r *benchmarkReport) write(completed bool) error {
	r.Completed = completed
	if err := r.writeJSON(); err != nil {
		return fmt.Errorf("write results.json: %w", err)
	}
	if err := r.writeCSV(); err != nil {
		return fmt.Errorf("write results.csv: %w", err)
	}
	return nil
}

func (r *benchmarkReport) writeJSON() error {
	path := filepath.Join(r.runDir, "results.json")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(r)
	closeErr := file.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}

func (r *benchmarkReport) writeCSV() error {
	file, err := os.Create(filepath.Join(r.runDir, "results.csv"))
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"scenario", "clients", "events_per_request", "requests", "events", "errors",
		"requests_per_second", "events_per_second", "p50_ms", "p95_ms", "p99_ms",
		"max_ms", "background_kind", "background_clients", "background_requests",
		"background_events", "background_errors", "background_requests_per_second",
		"background_events_per_second",
	}); err != nil {
		file.Close()
		return err
	}
	for _, result := range r.Results {
		if err := writer.Write([]string{
			result.Scenario,
			strconv.Itoa(result.Concurrency),
			strconv.Itoa(result.EventsPerRequest),
			strconv.FormatInt(result.Requests, 10),
			strconv.FormatInt(result.Events, 10),
			strconv.FormatInt(result.Errors, 10),
			fmt.Sprintf("%.3f", result.RequestsPerSecond),
			fmt.Sprintf("%.3f", result.EventsPerSecond),
			fmt.Sprintf("%.3f", float64(result.LatencyP50NS)/1e6),
			fmt.Sprintf("%.3f", float64(result.LatencyP95NS)/1e6),
			fmt.Sprintf("%.3f", float64(result.LatencyP99NS)/1e6),
			fmt.Sprintf("%.3f", float64(result.LatencyMaxNS)/1e6),
			result.BackgroundKind,
			strconv.Itoa(result.BackgroundClients),
			strconv.FormatInt(result.BackgroundRequests, 10),
			strconv.FormatInt(result.BackgroundEvents, 10),
			strconv.FormatInt(result.BackgroundErrors, 10),
			fmt.Sprintf("%.3f", result.BackgroundRequestsPerSecond),
			fmt.Sprintf("%.3f", result.BackgroundEventsPerSecond),
		}); err != nil {
			file.Close()
			return err
		}
	}
	writer.Flush()
	writeErr := writer.Error()
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
