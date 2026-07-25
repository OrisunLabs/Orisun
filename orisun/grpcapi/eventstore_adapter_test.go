//go:build !orisun_embedded

package grpcapi

import (
	"context"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type codedErrorSaver struct{}

func (codedErrorSaver) SavePrepared(
	context.Context,
	orisun.PreparedEventBatch,
	string,
	*orisun.Position,
	*orisun.Query,
) (string, int64, error) {
	return "", 0, statuscode.New(statuscode.AlreadyExists, "position conflict")
}

type mappingSaver struct {
	events   orisun.PreparedEventBatch
	boundary string
	position *orisun.Position
	query    *orisun.Query
}

func (s *mappingSaver) SavePrepared(
	_ context.Context,
	events orisun.PreparedEventBatch,
	boundary string,
	position *orisun.Position,
	query *orisun.Query,
) (string, int64, error) {
	s.events = events
	s.boundary = boundary
	s.position = position
	s.query = query
	return "12", 13, nil
}

type mappingRetriever struct {
	request *orisun.GetEventsRequest
	latest  orisun.LatestByCriteriaQuery
	created time.Time
}

func (r *mappingRetriever) GetBatch(_ context.Context, request *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	r.request = request
	return orisun.ReadEventBatch{{
		EventId: "event-1", EventType: "Opened", Data: `{"eventType":"Opened"}`, Metadata: `{}`,
		CommitPosition: 8, PreparePosition: 9, DateCreated: r.created,
	}}, nil
}

func (r *mappingRetriever) GetLatestByCriteria(
	_ context.Context,
	query orisun.LatestByCriteriaQuery,
) (orisun.LatestByCriteriaBatch, error) {
	r.latest = query
	return orisun.LatestByCriteriaBatch{
		Matches: []orisun.LatestCriterionMatch{{
			Found: true,
			Event: orisun.ReadEvent{
				EventId: "event-2", EventType: "Credited", Data: `{}`, Metadata: `{}`,
				CommitPosition: 10, PreparePosition: 11, DateCreated: r.created,
			},
		}},
		ContextCommitPosition:  10,
		ContextPreparePosition: 11,
	}, nil
}

func TestEventStoreAdapterTranslatesStatusAtGRPCBoundary(t *testing.T) {
	logger, err := logging.ZapLogger("error")
	if err != nil {
		t.Fatalf("logger: %v", err)
	}
	eventStore := orisun.NewEventStoreServer(
		nil, codedErrorSaver{}, nil, nil, nil,
		orisun.EventStreamConfig{}, logger,
	)

	_, err = AdaptEventStore(eventStore).SaveEvents(context.Background(), &SaveEventsRequest{
		Boundary: "test",
		Events: []*EventToSave{{
			EventId:   "event-1",
			EventType: "Created",
			Data:      `{}`,
			Metadata:  `{}`,
		}},
	})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("status.Code() = %v, want %v (err: %v)", got, codes.AlreadyExists, err)
	}
}

func TestEventStoreAdapterMapsSaveRequestAndResponse(t *testing.T) {
	logger, err := logging.ZapLogger("error")
	if err != nil {
		t.Fatal(err)
	}
	saver := &mappingSaver{}
	store := orisun.NewEventStoreServer(
		nil, saver, nil, nil, nil,
		orisun.EventStreamConfig{}, logger,
	)
	response, err := AdaptEventStore(store).SaveEvents(t.Context(), &SaveEventsRequest{
		Boundary: "orders",
		Query: &SaveQuery{
			ExpectedPosition: &Position{CommitPosition: 3, PreparePosition: 4},
			SubsetQuery: &Query{Criteria: []*Criterion{{
				Tags: []*Tag{{Key: "order_id", Value: "o-1"}},
			}}},
		},
		Events: []*EventToSave{{
			EventId: "event-1", EventType: "Opened", Data: `{"order_id":"o-1"}`, Metadata: `{}`,
		}},
	})
	if err != nil {
		t.Fatalf("SaveEvents() error = %v", err)
	}
	if response.LogPosition.CommitPosition != 12 || response.LogPosition.PreparePosition != 13 {
		t.Fatalf("SaveEvents() response = %#v", response)
	}
	if saver.boundary != "orders" || saver.position.CommitPosition != 3 ||
		saver.query.Criteria[0].Tags[0].Value != "o-1" ||
		saver.events[0].EventType != "Opened" {
		t.Fatalf("mapped save = %#v %#v %#v %#v", saver.boundary, saver.position, saver.query, saver.events)
	}
}

func TestEventStoreAdapterMapsReadAndLatestResponses(t *testing.T) {
	logger, err := logging.ZapLogger("error")
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, time.July, 23, 10, 0, 0, 123, time.UTC)
	retriever := &mappingRetriever{created: created}
	store := orisun.NewEventStoreServer(
		nil, nil, retriever, nil, nil,
		orisun.EventStreamConfig{}, logger,
	)
	adapter := AdaptEventStore(store)

	read, err := adapter.GetEvents(t.Context(), &GetEventsRequest{
		Boundary:     "orders",
		Count:        10,
		Direction:    Direction_DESC,
		FromPosition: &Position{CommitPosition: 2, PreparePosition: 3},
		Query: &Query{Criteria: []*Criterion{{
			Tags: []*Tag{{Key: "eventType", Value: "Opened"}},
		}}},
	})
	if err != nil {
		t.Fatalf("GetEvents() error = %v", err)
	}
	if retriever.request.Direction != orisun.Direction_DESC ||
		retriever.request.FromPosition.PreparePosition != 3 ||
		retriever.request.Query.Criteria[0].Tags[0].Value != "Opened" {
		t.Fatalf("mapped read request = %#v", retriever.request)
	}
	if len(read.Events) != 1 || read.Events[0].Position.PreparePosition != 9 ||
		!read.Events[0].DateCreated.AsTime().Equal(created) {
		t.Fatalf("mapped read response = %#v", read)
	}

	latest, err := adapter.GetLatestByCriteria(t.Context(), &GetLatestByCriteriaRequest{
		Boundary: "orders",
		Criteria: []*Criterion{{Tags: []*Tag{{Key: "account_id", Value: "a-1"}}}},
	})
	if err != nil {
		t.Fatalf("GetLatestByCriteria() error = %v", err)
	}
	if retriever.latest.Criteria[0].Tags[0].Value != "a-1" ||
		latest.Results[0].Event.EventId != "event-2" ||
		latest.ContextPosition.PreparePosition != 11 {
		t.Fatalf("mapped latest = %#v, query = %#v", latest, retriever.latest)
	}
}

func TestEventStoreAdapterReturnsServerInfoCopy(t *testing.T) {
	t.Parallel()

	info := ServerRuntimeInfo{
		Version:   "0.10.0",
		GitCommit: "abc123",
		BuildTime: "2026-07-25T12:00:00Z",
		Backend:   StorageBackend_STORAGE_BACKEND_SQLITE,
		NodeID:    "node-1",
		Capabilities: []ServerCapability{
			ServerCapability_SERVER_CAPABILITY_COMMAND_CONTEXT_CONSISTENCY,
			ServerCapability_SERVER_CAPABILITY_GRPC_HEALTH,
		},
	}
	adapter := AdaptEventStoreWithServerInfo(nil, info)
	info.Capabilities[0] = ServerCapability_SERVER_CAPABILITY_UNSPECIFIED

	response, err := adapter.GetServerInfo(t.Context(), &GetServerInfoRequest{})
	if err != nil {
		t.Fatalf("GetServerInfo() returned an error: %v", err)
	}
	if response.Version != "0.10.0" ||
		response.GitCommit != "abc123" ||
		response.BuildTime != "2026-07-25T12:00:00Z" ||
		response.Backend != StorageBackend_STORAGE_BACKEND_SQLITE ||
		response.NodeId != "node-1" {
		t.Fatalf("GetServerInfo() = %#v", response)
	}
	if len(response.Capabilities) != 2 ||
		response.Capabilities[0] != ServerCapability_SERVER_CAPABILITY_COMMAND_CONTEXT_CONSISTENCY ||
		response.Capabilities[1] != ServerCapability_SERVER_CAPABILITY_GRPC_HEALTH {
		t.Fatalf("GetServerInfo() capabilities = %v", response.Capabilities)
	}

	response.Capabilities[0] = ServerCapability_SERVER_CAPABILITY_UNSPECIFIED
	again, err := adapter.GetServerInfo(t.Context(), &GetServerInfoRequest{})
	if err != nil {
		t.Fatalf("second GetServerInfo() returned an error: %v", err)
	}
	if again.Capabilities[0] != ServerCapability_SERVER_CAPABILITY_COMMAND_CONTEXT_CONSISTENCY {
		t.Fatal("GetServerInfo returned mutable adapter state")
	}
}
