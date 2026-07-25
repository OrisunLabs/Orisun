//go:build !orisun_embedded

package orisun

import (
	"context"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestEventStoreMetricsRecordSuccessfulCommitEventsBytesAndDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	metrics, err := newEventStoreMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatalf("newEventStoreMetrics() error = %v", err)
	}
	saver := &metricsSaver{transactionID: "7", globalID: 8}
	store := &EventStore{
		saveEventsFn: saver,
		logger:       noopLogger{},
	}
	store.metrics.Store(metrics)

	_, err = store.SaveEvents(t.Context(), &SaveEventsRequest{
		Boundary: "orders",
		Events: []*EventToSave{
			{
				EventId:   "event-1",
				EventType: "OrderPlaced",
				Data:      `{"order_id":"order-1"}`,
				Metadata:  `{"source":"checkout"}`,
			},
			{
				EventId:   "event-2",
				EventType: "OrderConfirmed",
				Data:      `{"order_id":"order-1"}`,
				Metadata:  `{}`,
			},
		},
	})
	if err != nil {
		t.Fatalf("SaveEvents() error = %v", err)
	}

	collected := collectEventStoreMetrics(t, reader)
	assertInt64MetricPoint(
		t,
		eventStoreMetricByName(t, collected, "orisun.eventstore.commits"),
		1,
		"orisun.boundary.name",
		"orders",
	)
	assertInt64MetricPoint(
		t,
		eventStoreMetricByName(t, collected, "orisun.eventstore.events"),
		2,
		"orisun.boundary.name",
		"orders",
	)
	assertInt64MetricPoint(
		t,
		eventStoreMetricByName(t, collected, "orisun.eventstore.payload.size"),
		preparedPayloadBytes(saver.prepared),
		"orisun.boundary.name",
		"orders",
	)

	duration := eventStoreMetricByName(t, collected, "orisun.eventstore.commit.duration")
	durationData, ok := duration.Data.(metricdata.Histogram[float64])
	if !ok || len(durationData.DataPoints) != 1 || durationData.DataPoints[0].Count != 1 {
		t.Fatalf("commit duration metric = %#v", duration.Data)
	}
	if len(durationData.DataPoints[0].Bounds) != len(commitDurationBuckets) {
		t.Fatalf("commit duration bounds = %v", durationData.DataPoints[0].Bounds)
	}
	assertEventStoreMetricAttribute(
		t,
		durationData.DataPoints[0].Attributes,
		"orisun.eventstore.commit.status",
		"OK",
	)
}

func TestEventStoreMetricsRecordCCCConflictWithoutCriterionValues(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	metrics, err := newEventStoreMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatalf("newEventStoreMetrics() error = %v", err)
	}
	store := &EventStore{
		saveEventsFn: &metricsSaver{
			err: statuscode.New(statuscode.AlreadyExists, "consistency context changed"),
		},
		logger: noopLogger{},
	}
	store.metrics.Store(metrics)

	_, err = store.SaveEvents(t.Context(), &SaveEventsRequest{
		Boundary: "orders",
		Query: &SaveQuery{
			ExpectedPosition: &Position{CommitPosition: 2, PreparePosition: 3},
			SubsetQuery: &Query{Criteria: []*Criterion{
				{Tags: []*Tag{{Key: "customer_id", Value: "secret-customer-42"}}},
				{Tags: []*Tag{{Key: "region", Value: "secret-region"}}},
			}},
		},
		Events: []*EventToSave{{
			EventId:   "event-1",
			EventType: "OrderPlaced",
			Data:      `{}`,
			Metadata:  `{}`,
		}},
	})
	if statuscode.CodeOf(err) != statuscode.AlreadyExists {
		t.Fatalf("SaveEvents() error = %v, want AlreadyExists", err)
	}

	collected := collectEventStoreMetrics(t, reader)
	conflicts := eventStoreMetricByName(t, collected, "orisun.ccc.conflicts")
	conflictData, ok := conflicts.Data.(metricdata.Sum[int64])
	if !ok || len(conflictData.DataPoints) != 1 || conflictData.DataPoints[0].Value != 1 {
		t.Fatalf("CCC conflict metric = %#v", conflicts.Data)
	}
	point := conflictData.DataPoints[0]
	assertEventStoreMetricAttribute(t, point.Attributes, "orisun.boundary.name", "orders")
	assertEventStoreMetricAttribute(
		t,
		point.Attributes,
		"orisun.ccc.criterion_shape",
		"multiple_criteria",
	)
	for _, item := range point.Attributes.ToSlice() {
		if item.Value.AsString() == "secret-customer-42" || item.Value.AsString() == "secret-region" {
			t.Fatalf("criterion value leaked into metric attributes: %v", item)
		}
	}

	duration := eventStoreMetricByName(t, collected, "orisun.eventstore.commit.duration")
	durationData, ok := duration.Data.(metricdata.Histogram[float64])
	if !ok || len(durationData.DataPoints) != 1 {
		t.Fatalf("commit duration metric = %#v", duration.Data)
	}
	assertEventStoreMetricAttribute(
		t,
		durationData.DataPoints[0].Attributes,
		"orisun.eventstore.commit.status",
		"ALREADY_EXISTS",
	)
	assertEventStoreMetricAttribute(
		t,
		durationData.DataPoints[0].Attributes,
		"error.type",
		"ALREADY_EXISTS",
	)
}

func TestEventStoreMetricsAreDisabledByDefault(t *testing.T) {
	store := NewEventStoreServer(
		nil,
		&metricsSaver{},
		nil,
		nil,
		nil,
		EventStreamConfig{},
		noopLogger{},
	)

	if metrics := store.metrics.Load(); metrics != nil {
		t.Fatalf("event-store metrics = %p, want nil before telemetry is enabled", metrics)
	}
}

func TestEventStoreMetricsReuseCachedAttributeOptions(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })
	metrics, err := newEventStoreMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatalf("newEventStoreMetrics() error = %v", err)
	}

	if first, second := metrics.boundaryOptionsFor("orders"), metrics.boundaryOptionsFor("orders"); first[0] != second[0] {
		t.Fatal("boundary metric option was not reused")
	}
	if first, second := metrics.commitOptionsFor("orders", "OK"), metrics.commitOptionsFor("orders", "OK"); first[0] != second[0] {
		t.Fatal("commit metric option was not reused")
	}
	if first, second := metrics.conflictOptionsFor("orders", "position_only"), metrics.conflictOptionsFor("orders", "position_only"); first[0] != second[0] {
		t.Fatal("conflict metric option was not reused")
	}
}

func BenchmarkSavePreparedMetricsGate(b *testing.B) {
	ctx := context.Background()
	saver := &metricsSaver{transactionID: "1", globalID: 1}
	events := PreparedEventBatch{{
		EventId:      "event-1",
		EventType:    "OrderPlaced",
		DataJSON:     `{"eventType":"OrderPlaced","order_id":"order-1"}`,
		MetadataJSON: `{}`,
	}}

	b.Run("baseline", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _, _ = saver.SavePrepared(ctx, events, "orders", nil, nil)
		}
	})

	b.Run("disabled", func(b *testing.B) {
		store := &EventStore{}
		b.ReportAllocs()
		for range b.N {
			_, _, _ = store.savePreparedWithMetrics(
				ctx,
				saver,
				events,
				"orders",
				nil,
				nil,
			)
		}
	})

	b.Run("enabled_cached", func(b *testing.B) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		b.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		metrics, err := newEventStoreMetrics(provider.Meter("benchmark"))
		if err != nil {
			b.Fatalf("newEventStoreMetrics() error = %v", err)
		}
		store := &EventStore{}
		store.metrics.Store(metrics)
		metrics.boundaryOptionsFor("orders")
		metrics.commitOptionsFor("orders", "OK")

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _, _ = store.savePreparedWithMetrics(
				ctx,
				saver,
				events,
				"orders",
				nil,
				nil,
			)
		}
	})
}

type metricsSaver struct {
	transactionID string
	globalID      int64
	err           error
	prepared      PreparedEventBatch
}

func (s *metricsSaver) SavePrepared(
	_ context.Context,
	events PreparedEventBatch,
	_ string,
	_ *Position,
	_ *Query,
) (string, int64, error) {
	s.prepared = events
	return s.transactionID, s.globalID, s.err
}

func collectEventStoreMetrics(
	t *testing.T,
	reader *sdkmetric.ManualReader,
) metricdata.ResourceMetrics {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	return collected
}

func eventStoreMetricByName(
	t *testing.T,
	collected metricdata.ResourceMetrics,
	name string,
) metricdata.Metrics {
	t.Helper()
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func assertInt64MetricPoint(
	t *testing.T,
	metric metricdata.Metrics,
	want int64,
	attributeKey string,
	attributeValue string,
) {
	t.Helper()
	data, ok := metric.Data.(metricdata.Sum[int64])
	if !ok || len(data.DataPoints) != 1 || data.DataPoints[0].Value != want {
		t.Fatalf("metric %q = %#v, want %d", metric.Name, metric.Data, want)
	}
	assertEventStoreMetricAttribute(
		t,
		data.DataPoints[0].Attributes,
		attributeKey,
		attributeValue,
	)
}

func assertEventStoreMetricAttribute(
	t *testing.T,
	attributes attribute.Set,
	key string,
	want string,
) {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(key))
	if !ok || value.AsString() != want {
		t.Fatalf("attribute %q = %q, want %q", key, value.AsString(), want)
	}
}
