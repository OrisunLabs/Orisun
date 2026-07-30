package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type scenario struct {
	Name             string
	Kind             string
	EventsPerRequest int
	BackgroundKind   string
}

type benchmarkResult struct {
	Boundary                    string           `json:"boundary"`
	Scenario                    string           `json:"scenario"`
	Kind                        string           `json:"kind"`
	Concurrency                 int              `json:"concurrency"`
	BackgroundKind              string           `json:"background_kind,omitempty"`
	BackgroundClients           int              `json:"background_clients,omitempty"`
	EventsPerRequest            int              `json:"events_per_request,omitempty"`
	DurationNS                  int64            `json:"duration_ns"`
	Requests                    int64            `json:"requests"`
	Events                      int64            `json:"events"`
	Errors                      int64            `json:"errors"`
	RequestsPerSecond           float64          `json:"requests_per_second"`
	EventsPerSecond             float64          `json:"events_per_second"`
	ErrorRate                   float64          `json:"error_rate"`
	LatencyP50NS                int64            `json:"latency_p50_ns"`
	LatencyP95NS                int64            `json:"latency_p95_ns"`
	LatencyP99NS                int64            `json:"latency_p99_ns"`
	LatencyMaxNS                int64            `json:"latency_max_ns"`
	ErrorBreakdown              map[string]int64 `json:"error_breakdown,omitempty"`
	BackgroundRequests          int64            `json:"background_requests"`
	BackgroundEvents            int64            `json:"background_events"`
	BackgroundErrors            int64            `json:"background_errors"`
	BackgroundRequestsPerSecond float64          `json:"background_requests_per_second"`
	BackgroundEventsPerSecond   float64          `json:"background_events_per_second"`
	BackgroundErrorBreakdown    map[string]int64 `json:"background_error_breakdown,omitempty"`
}

type workerStats struct {
	requests   int64
	events     int64
	errors     int64
	latency    []int64
	errorKinds map[string]int64
}

type operationState struct {
	readCursor *grpcapi.Position
}

type idSource struct {
	next uint64
}

func (s *idSource) take() uint64 {
	return atomic.AddUint64(&s.next, 1)
}

var availableScenarios = []scenario{
	{Name: "write-conditional-1", Kind: "write-conditional", EventsPerRequest: 1},
	{Name: "write-conditional-10", Kind: "write-conditional", EventsPerRequest: 10},
	{Name: "write-conditional-100", Kind: "write-conditional", EventsPerRequest: 100},
	{Name: "write-unconditional-1", Kind: "write-unconditional", EventsPerRequest: 1},
	{Name: "write-unconditional-10", Kind: "write-unconditional", EventsPerRequest: 10},
	{Name: "write-unconditional-100", Kind: "write-unconditional", EventsPerRequest: 100},
	{Name: "write-unconditional-1000", Kind: "write-unconditional", EventsPerRequest: 1000},
	{Name: "read-conditional", Kind: "read-conditional"},
	{Name: "read-unconditional", Kind: "read-unconditional"},
	{Name: "read-unconditional-constrained", Kind: "read-unconditional-constrained"},
	{
		Name:             "write-unconditional-1-with-readers",
		Kind:             "write-unconditional",
		EventsPerRequest: 1,
		BackgroundKind:   "read-unconditional",
	},
	{
		Name:           "read-unconditional-with-writers",
		Kind:           "read-unconditional",
		BackgroundKind: "write-unconditional",
	},
}

func selectScenarios(value string) ([]scenario, error) {
	if strings.TrimSpace(value) == "all" {
		return append([]scenario(nil), availableScenarios...), nil
	}
	wanted := make(map[string]struct{})
	for _, name := range strings.Split(value, ",") {
		wanted[strings.TrimSpace(name)] = struct{}{}
	}
	selected := make([]scenario, 0, len(wanted))
	for _, candidate := range availableScenarios {
		if _, ok := wanted[candidate.Name]; ok {
			selected = append(selected, candidate)
			delete(wanted, candidate.Name)
		}
	}
	if len(wanted) > 0 {
		unknown := make([]string, 0, len(wanted))
		for name := range wanted {
			unknown = append(unknown, name)
		}
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown scenarios: %s", strings.Join(unknown, ", "))
	}
	return selected, nil
}

func (c *benchmarkClient) prepopulate(ctx context.Context) error {
	fmt.Printf(
		"prepopulating %d events across %d contexts...\n",
		c.config.PrepopulateEvents,
		c.config.TagCardinality,
	)
	for offset := 0; offset < c.config.PrepopulateEvents; offset += c.config.PrepopulateBatch {
		count := min(c.config.PrepopulateBatch, c.config.PrepopulateEvents-offset)
		events := make([]*grpcapi.EventToSave, count)
		for i := range count {
			contextID := (offset + i) % c.config.TagCardinality
			events[i] = c.makeEvent("BenchmarkSeed", fmt.Sprintf("seed-%d", contextID), offset+i)
		}
		requestCtx, cancel := c.requestContext(ctx)
		_, err := c.eventStore.SaveEvents(requestCtx, &grpcapi.SaveEventsRequest{
			Boundary: c.config.Boundary,
			Events:   events,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("prepopulate at event %d: %w", offset, err)
		}
		if offset > 0 && offset%(100*c.config.PrepopulateBatch) == 0 {
			fmt.Printf("  %d / %d\n", offset, c.config.PrepopulateEvents)
		}
	}
	return nil
}

func (c *benchmarkClient) measureScenario(
	ctx context.Context,
	scenario scenario,
	concurrency int,
) benchmarkResult {
	if c.config.Warmup > 0 {
		warmupCtx, cancel := context.WithTimeout(ctx, c.config.Warmup)
		c.runScenarioPhase(warmupCtx, scenario, concurrency, false)
		cancel()
	}

	measuredCtx, cancel := context.WithTimeout(ctx, c.config.Duration)
	stats, backgroundStats, elapsed := c.runScenarioPhase(
		measuredCtx,
		scenario,
		concurrency,
		true,
	)
	cancel()

	result := benchmarkResult{
		Boundary:                 c.config.Boundary,
		Scenario:                 scenario.Name,
		Kind:                     scenario.Kind,
		Concurrency:              concurrency,
		EventsPerRequest:         scenario.EventsPerRequest,
		DurationNS:               elapsed.Nanoseconds(),
		Requests:                 stats.requests,
		Events:                   stats.events,
		Errors:                   stats.errors,
		ErrorBreakdown:           stats.errorKinds,
		BackgroundKind:           scenario.BackgroundKind,
		BackgroundClients:        0,
		BackgroundRequests:       backgroundStats.requests,
		BackgroundEvents:         backgroundStats.events,
		BackgroundErrors:         backgroundStats.errors,
		BackgroundErrorBreakdown: backgroundStats.errorKinds,
	}
	if scenario.BackgroundKind != "" {
		result.BackgroundClients = c.config.OppositeClients
	}
	if elapsed > 0 {
		result.RequestsPerSecond = float64(stats.requests) / elapsed.Seconds()
		result.EventsPerSecond = float64(stats.events) / elapsed.Seconds()
		result.BackgroundRequestsPerSecond = float64(backgroundStats.requests) / elapsed.Seconds()
		result.BackgroundEventsPerSecond = float64(backgroundStats.events) / elapsed.Seconds()
	}
	attempts := stats.requests + stats.errors
	if attempts > 0 {
		result.ErrorRate = float64(stats.errors) / float64(attempts)
	}
	if len(stats.latency) > 0 {
		sort.Slice(stats.latency, func(i, j int) bool { return stats.latency[i] < stats.latency[j] })
		result.LatencyP50NS = percentile(stats.latency, 0.50)
		result.LatencyP95NS = percentile(stats.latency, 0.95)
		result.LatencyP99NS = percentile(stats.latency, 0.99)
		result.LatencyMaxNS = stats.latency[len(stats.latency)-1]
	}
	return result
}

func (c *benchmarkClient) runScenarioPhase(
	ctx context.Context,
	workload scenario,
	concurrency int,
	recordLatency bool,
) (workerStats, workerStats, time.Duration) {
	start := make(chan struct{})
	foregroundReady := make(chan struct{})
	foregroundDone := make(chan workerStats, 1)
	go func() {
		foregroundDone <- c.runWorkersOnSignal(
			ctx,
			workload,
			concurrency,
			recordLatency,
			start,
			foregroundReady,
		)
	}()

	var backgroundDone chan workerStats
	var backgroundReady chan struct{}
	if workload.BackgroundKind != "" {
		backgroundDone = make(chan workerStats, 1)
		backgroundReady = make(chan struct{})
		backgroundScenario := scenario{
			Name:             workload.Name + "-background",
			Kind:             workload.BackgroundKind,
			EventsPerRequest: 1,
		}
		go func() {
			backgroundDone <- c.runWorkersOnSignal(
				ctx,
				backgroundScenario,
				c.config.OppositeClients,
				false,
				start,
				backgroundReady,
			)
		}()
	}

	<-foregroundReady
	if backgroundReady != nil {
		<-backgroundReady
	}
	started := time.Now()
	close(start)
	foreground := <-foregroundDone
	elapsed := time.Since(started)
	background := workerStats{}
	if backgroundDone != nil {
		background = <-backgroundDone
	}
	return foreground, background, elapsed
}

func (c *benchmarkClient) runWorkersOnSignal(
	ctx context.Context,
	scenario scenario,
	concurrency int,
	recordLatency bool,
	start <-chan struct{},
	ready chan<- struct{},
) workerStats {
	results := make(chan workerStats, concurrency)
	var wg sync.WaitGroup
	for worker := range concurrency {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			local := workerStats{}
			state := operationState{}
			if recordLatency {
				local.latency = make([]int64, 0, 4096)
			}
			<-start
			for ctx.Err() == nil {
				started := time.Now()
				events, err := c.executeOperation(
					ctx,
					scenario.Kind,
					scenario.EventsPerRequest,
					worker,
					&state,
				)
				latency := time.Since(started)
				if measurementEnded(ctx, err) {
					break
				}
				if err != nil {
					local.errors++
					if local.errorKinds == nil {
						local.errorKinds = make(map[string]int64)
					}
					local.errorKinds[status.Code(err).String()]++
					continue
				}
				local.requests++
				local.events += int64(events)
				if recordLatency {
					local.latency = append(local.latency, latency.Nanoseconds())
				}
				if scenario.Kind == "read-unconditional-constrained" && events > 0 {
					target := time.Duration(float64(time.Second) *
						float64(events) / float64(c.config.ConstrainedReadRate))
					if remaining := target - latency; remaining > 0 {
						timer := time.NewTimer(remaining)
						select {
						case <-ctx.Done():
							timer.Stop()
						case <-timer.C:
						}
					}
				}
			}
			results <- local
		}(worker)
	}
	close(ready)
	wg.Wait()
	close(results)

	total := workerStats{}
	for local := range results {
		total.requests += local.requests
		total.events += local.events
		total.errors += local.errors
		total.latency = append(total.latency, local.latency...)
		for kind, count := range local.errorKinds {
			if total.errorKinds == nil {
				total.errorKinds = make(map[string]int64)
			}
			total.errorKinds[kind] += count
		}
	}
	return total
}

func measurementEnded(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if err == nil {
		return false
	}
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline || time.Now().Before(deadline) {
		return false
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func (c *benchmarkClient) executeOperation(
	ctx context.Context,
	kind string,
	eventsPerRequest int,
	worker int,
	state *operationState,
) (int, error) {
	switch kind {
	case "write-unconditional":
		return c.write(ctx, eventsPerRequest, false)
	case "write-conditional":
		return c.write(ctx, eventsPerRequest, true)
	case "read-conditional":
		return c.readConditional(ctx, worker)
	case "read-unconditional", "read-unconditional-constrained":
		return c.readUnconditional(ctx, state)
	default:
		return 0, fmt.Errorf("unsupported operation kind %q", kind)
	}
}

func (c *benchmarkClient) write(
	ctx context.Context,
	eventsPerRequest int,
	conditional bool,
) (int, error) {
	operationID := c.ids.take()
	contextValue := fmt.Sprintf("write-%d", operationID)
	events := make([]*grpcapi.EventToSave, eventsPerRequest)
	for i := range eventsPerRequest {
		events[i] = c.makeEvent("BenchmarkWrite", contextValue, i)
	}
	request := &grpcapi.SaveEventsRequest{
		Boundary: c.config.Boundary,
		Events:   events,
	}
	if conditional {
		request.Query = &grpcapi.SaveQuery{
			ExpectedPosition: &grpcapi.Position{
				CommitPosition:  -1,
				PreparePosition: -1,
			},
			SubsetQuery: &grpcapi.Query{Criteria: []*grpcapi.Criterion{{
				Tags: []*grpcapi.Tag{{Key: "benchmark_context", Value: contextValue}},
			}}},
		}
	}
	requestCtx, cancel := c.requestContext(ctx)
	_, err := c.eventStore.SaveEvents(requestCtx, request)
	cancel()
	if err != nil {
		return 0, err
	}
	return eventsPerRequest, nil
}

func (c *benchmarkClient) readConditional(ctx context.Context, worker int) (int, error) {
	contextID := int(c.ids.take()+uint64(worker)) % c.config.TagCardinality
	requestCtx, cancel := c.requestContext(ctx)
	response, err := c.eventStore.GetEvents(requestCtx, &grpcapi.GetEventsRequest{
		Boundary: c.config.Boundary,
		Count:    uint32(min(c.config.ReadBatch, 10_000)),
		Query: &grpcapi.Query{Criteria: []*grpcapi.Criterion{{
			Tags: []*grpcapi.Tag{{
				Key:   "benchmark_context",
				Value: fmt.Sprintf("seed-%d", contextID),
			}},
		}}},
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return len(response.Events), nil
}

func (c *benchmarkClient) readUnconditional(
	ctx context.Context,
	state *operationState,
) (int, error) {
	requestCtx, cancel := c.requestContext(ctx)
	response, err := c.eventStore.GetEvents(requestCtx, &grpcapi.GetEventsRequest{
		Boundary:     c.config.Boundary,
		Count:        uint32(c.config.ReadBatch),
		Direction:    grpcapi.Direction_ASC,
		FromPosition: state.readCursor,
	})
	cancel()
	if err != nil {
		return 0, err
	}
	if len(response.Events) == 0 {
		state.readCursor = nil
		return 0, nil
	}
	position := response.Events[len(response.Events)-1].Position
	if position == nil {
		return 0, errors.New("unconditional read returned an event without a position")
	}
	state.readCursor = &grpcapi.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition + 1,
	}
	return len(response.Events), nil
}

func (c *benchmarkClient) makeEvent(
	eventType string,
	contextValue string,
	index int,
) *grpcapi.EventToSave {
	id := c.ids.take()
	base := fmt.Sprintf(
		`{"benchmark_context":%q,"event_index":%d,"sequence":%d,"payload":"`,
		contextValue,
		index,
		id,
	)
	padding := c.config.PayloadBytes - len(base) - 2
	if padding < 0 {
		padding = 0
	}
	return &grpcapi.EventToSave{
		EventId:   fmt.Sprintf("%08x-0000-7000-8000-%012x", uint32(id>>48), id&0xffffffffffff),
		EventType: eventType,
		Data:      base + strings.Repeat("x", padding) + `"}`,
		Metadata:  `{"source":"orisun-bench"}`,
	}
}

func percentile(sorted []int64, quantile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted))*quantile)) - 1
	index = max(0, min(index, len(sorted)-1))
	return sorted[index]
}
