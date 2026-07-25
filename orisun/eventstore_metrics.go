//go:build !orisun_embedded

package orisun

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const eventStoreInstrumentationName = "github.com/OrisunLabs/Orisun/orisun"

var commitDurationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
	0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
}

type eventStoreMetrics struct {
	commits   otelmetric.Int64Counter
	events    otelmetric.Int64Counter
	bytes     otelmetric.Int64Counter
	duration  otelmetric.Float64Histogram
	conflicts otelmetric.Int64Counter

	boundaryOptions sync.Map
	commitOptions   sync.Map
	conflictOptions sync.Map
}

type commitAttributeKey struct {
	boundary string
	status   string
}

type conflictAttributeKey struct {
	boundary       string
	criterionShape string
}

func newEventStoreMetrics(meter otelmetric.Meter) (*eventStoreMetrics, error) {
	commits, err := meter.Int64Counter(
		"orisun.eventstore.commits",
		otelmetric.WithDescription("Number of event batches committed successfully."),
		otelmetric.WithUnit("{commit}"),
	)
	if err != nil {
		return nil, err
	}
	events, err := meter.Int64Counter(
		"orisun.eventstore.events",
		otelmetric.WithDescription("Number of events committed successfully."),
		otelmetric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}
	bytes, err := meter.Int64Counter(
		"orisun.eventstore.payload.size",
		otelmetric.WithDescription("Uncompressed event data and metadata bytes committed successfully."),
		otelmetric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram(
		"orisun.eventstore.commit.duration",
		otelmetric.WithDescription("Duration of a durable event-store commit attempt."),
		otelmetric.WithUnit("s"),
		otelmetric.WithExplicitBucketBoundaries(commitDurationBuckets...),
	)
	if err != nil {
		return nil, err
	}
	conflicts, err := meter.Int64Counter(
		"orisun.ccc.conflicts",
		otelmetric.WithDescription("Number of Command Context Consistency conflicts."),
		otelmetric.WithUnit("{conflict}"),
	)
	if err != nil {
		return nil, err
	}
	return &eventStoreMetrics{
		commits:   commits,
		events:    events,
		bytes:     bytes,
		duration:  duration,
		conflicts: conflicts,
	}, nil
}

func defaultEventStoreMetrics() (*eventStoreMetrics, error) {
	return newEventStoreMetrics(otel.Meter(eventStoreInstrumentationName))
}

// EnableOpenTelemetryMetrics attaches event-store instruments to the current
// global OpenTelemetry MeterProvider. Metrics are disabled until this is called.
func (s *EventStore) EnableOpenTelemetryMetrics() error {
	if s.metrics.Load() != nil {
		return nil
	}
	metrics, err := defaultEventStoreMetrics()
	if err != nil {
		return err
	}
	s.metrics.CompareAndSwap(nil, metrics)
	return nil
}

func (s *EventStore) savePreparedWithMetrics(
	ctx context.Context,
	saver EventsSaver,
	events PreparedEventBatch,
	boundary string,
	expectedPosition *Position,
	subset *Query,
) (transactionID string, globalID int64, err error) {
	m := s.metrics.Load()
	if m == nil {
		transactionID, globalID, err = saver.SavePrepared(
			ctx, events, boundary, expectedPosition, subset,
		)
		if err != nil {
			err = normalizeSaveError(err)
		}
		return transactionID, globalID, err
	}

	started := time.Now()
	transactionID, globalID, err = saver.SavePrepared(
		ctx, events, boundary, expectedPosition, subset,
	)
	if err != nil {
		err = normalizeSaveError(err)
	}
	m.recordCommitAttempt(ctx, boundary, events, expectedPosition, subset, started, err)
	return transactionID, globalID, err
}

func (m *eventStoreMetrics) recordCommitAttempt(
	ctx context.Context,
	boundary string,
	events PreparedEventBatch,
	expectedPosition *Position,
	subset *Query,
	started time.Time,
	err error,
) {
	status := commitStatus(err)
	m.duration.Record(
		ctx,
		time.Since(started).Seconds(),
		m.commitOptionsFor(boundary, status)...,
	)

	if err == nil {
		boundaryOptions := m.boundaryOptionsFor(boundary)
		m.commits.Add(ctx, 1, boundaryOptions...)
		m.events.Add(ctx, int64(len(events)), boundaryOptions...)
		m.bytes.Add(ctx, preparedPayloadBytes(events), boundaryOptions...)
		return
	}
	if status == "ALREADY_EXISTS" {
		m.conflicts.Add(ctx, 1, m.conflictOptionsFor(
			boundary,
			cccCriterionShape(expectedPosition, subset),
		)...)
	}
}

func (m *eventStoreMetrics) boundaryOptionsFor(boundary string) []otelmetric.AddOption {
	if cached, ok := m.boundaryOptions.Load(boundary); ok {
		return cached.([]otelmetric.AddOption)
	}
	options := []otelmetric.AddOption{otelmetric.WithAttributeSet(attribute.NewSet(
		attribute.String("orisun.boundary.name", boundary),
	))}
	actual, _ := m.boundaryOptions.LoadOrStore(boundary, options)
	return actual.([]otelmetric.AddOption)
}

func (m *eventStoreMetrics) commitOptionsFor(
	boundary string,
	status string,
) []otelmetric.RecordOption {
	key := commitAttributeKey{boundary: boundary, status: status}
	if cached, ok := m.commitOptions.Load(key); ok {
		return cached.([]otelmetric.RecordOption)
	}
	attributes := []attribute.KeyValue{
		attribute.String("orisun.boundary.name", boundary),
		attribute.String("orisun.eventstore.commit.status", status),
	}
	if status != "OK" {
		attributes = append(attributes, attribute.String("error.type", status))
	}
	options := []otelmetric.RecordOption{
		otelmetric.WithAttributeSet(attribute.NewSet(attributes...)),
	}
	actual, _ := m.commitOptions.LoadOrStore(key, options)
	return actual.([]otelmetric.RecordOption)
}

func (m *eventStoreMetrics) conflictOptionsFor(
	boundary string,
	criterionShape string,
) []otelmetric.AddOption {
	key := conflictAttributeKey{boundary: boundary, criterionShape: criterionShape}
	if cached, ok := m.conflictOptions.Load(key); ok {
		return cached.([]otelmetric.AddOption)
	}
	options := []otelmetric.AddOption{otelmetric.WithAttributeSet(attribute.NewSet(
		attribute.String("orisun.boundary.name", boundary),
		attribute.String("orisun.ccc.criterion_shape", criterionShape),
	))}
	actual, _ := m.conflictOptions.LoadOrStore(key, options)
	return actual.([]otelmetric.AddOption)
}

func preparedPayloadBytes(events PreparedEventBatch) int64 {
	var size int64
	for _, event := range events {
		size += int64(len(event.DataJSON) + len(event.MetadataJSON))
	}
	return size
}

func cccCriterionShape(expectedPosition *Position, subset *Query) string {
	if subset == nil || len(subset.Criteria) == 0 {
		if expectedPosition != nil {
			return "position_only"
		}
		return "unscoped"
	}
	if len(subset.Criteria) > 1 {
		return "multiple_criteria"
	}
	if subset.Criteria[0] == nil || len(subset.Criteria[0].Tags) == 0 {
		return "empty_criterion"
	}
	if len(subset.Criteria[0].Tags) == 1 {
		return "single_criterion_single_tag"
	}
	return "single_criterion_multiple_tags"
}

func commitStatus(err error) string {
	switch {
	case err == nil:
		return "OK"
	case errors.Is(err, context.Canceled):
		return "CANCELLED"
	case errors.Is(err, context.DeadlineExceeded):
		return "DEADLINE_EXCEEDED"
	}

	switch statuscode.CodeOf(err) {
	case statuscode.Canceled:
		return "CANCELLED"
	case statuscode.InvalidArgument:
		return "INVALID_ARGUMENT"
	case statuscode.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case statuscode.NotFound:
		return "NOT_FOUND"
	case statuscode.AlreadyExists:
		return "ALREADY_EXISTS"
	case statuscode.PermissionDenied:
		return "PERMISSION_DENIED"
	case statuscode.Unauthenticated:
		return "UNAUTHENTICATED"
	case statuscode.Unavailable:
		return "UNAVAILABLE"
	case statuscode.Unimplemented:
		return "UNIMPLEMENTED"
	case statuscode.FailedPrecondition:
		return "FAILED_PRECONDITION"
	default:
		return "INTERNAL"
	}
}

func normalizeSaveError(err error) error {
	if err == nil {
		return nil
	}
	if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
		return err
	}
	if strings.Contains(err.Error(), "OptimisticConcurrencyException") {
		return statuscode.New(statuscode.AlreadyExists, err.Error())
	}
	return err
}
