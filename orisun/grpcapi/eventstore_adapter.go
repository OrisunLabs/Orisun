//go:build !orisun_embedded

package grpcapi

import (
	"context"
	"fmt"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/grpcstatus"
	"github.com/OrisunLabs/Orisun/orisun"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventStoreAdapter owns all conversion between generated protobuf messages
// and the transport-neutral EventStore implementation.
type EventStoreAdapter struct {
	UnimplementedEventStoreServer
	eventStore *orisun.EventStore
	serverInfo ServerRuntimeInfo
}

type ServerRuntimeInfo struct {
	Version      string
	GitCommit    string
	BuildTime    string
	Backend      StorageBackend
	NodeID       string
	Capabilities []ServerCapability
}

func AdaptEventStore(eventStore *orisun.EventStore) *EventStoreAdapter {
	version, buildTime, gitCommit := orisun.GetBuildInfo()
	return AdaptEventStoreWithServerInfo(eventStore, ServerRuntimeInfo{
		Version:   version,
		GitCommit: gitCommit,
		BuildTime: buildTime,
	})
}

func AdaptEventStoreWithServerInfo(eventStore *orisun.EventStore, info ServerRuntimeInfo) *EventStoreAdapter {
	info.Capabilities = append([]ServerCapability(nil), info.Capabilities...)
	return &EventStoreAdapter{
		eventStore: eventStore,
		serverInfo: info,
	}
}

func (a *EventStoreAdapter) SaveEvents(ctx context.Context, req *SaveEventsRequest) (*WriteResult, error) {
	response, err := a.eventStore.SaveEvents(ctx, saveEventsRequestFromProto(req))
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return writeResultToProto(response), nil
}

func (a *EventStoreAdapter) GetEvents(ctx context.Context, req *GetEventsRequest) (*GetEventsResponse, error) {
	response, err := a.eventStore.GetEvents(ctx, getEventsRequestFromProto(req))
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return getEventsResponseToProto(response), nil
}

func (a *EventStoreAdapter) GetLatestByCriteria(
	ctx context.Context,
	req *GetLatestByCriteriaRequest,
) (*GetLatestByCriteriaResponse, error) {
	response, err := a.eventStore.GetLatestByCriteria(ctx, latestRequestFromProto(req))
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return latestResponseToProto(response), nil
}

func (a *EventStoreAdapter) CatchUpSubscribeToEvents(
	req *CatchUpSubscribeToEventStoreRequest,
	stream EventStore_CatchUpSubscribeToEventsServer,
) error {
	if req == nil {
		return grpcstatus.FromError(fmt.Errorf("subscription request is required"))
	}
	return grpcstatus.FromError(a.eventStore.SubscribeToAllEvents(
		stream.Context(),
		coreeventstore.SubscribeRequest{
			Boundary:       req.Boundary,
			SubscriberName: req.SubscriberName,
			AfterPosition:  corePositionFromProto(req.AfterPosition),
			Query:          coreQueryFromProto(req.Query),
		},
		func(_ context.Context, event coreeventstore.ReadEvent) error {
			if err := stream.Send(readEventToProto(event)); err != nil {
				return fmt.Errorf("send event to subscription stream: %w", err)
			}
			return nil
		},
	))
}

func (a *EventStoreAdapter) Ping(ctx context.Context, _ *PingRequest) (*PingResponse, error) {
	if err := a.eventStore.Ping(ctx); err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return &PingResponse{}, nil
}

func (a *EventStoreAdapter) GetServerInfo(context.Context, *GetServerInfoRequest) (*GetServerInfoResponse, error) {
	return &GetServerInfoResponse{
		Version:      a.serverInfo.Version,
		GitCommit:    a.serverInfo.GitCommit,
		BuildTime:    a.serverInfo.BuildTime,
		Backend:      a.serverInfo.Backend,
		NodeId:       a.serverInfo.NodeID,
		Capabilities: append([]ServerCapability(nil), a.serverInfo.Capabilities...),
	}, nil
}

func (a *EventStoreAdapter) CreateIndex(ctx context.Context, req *CreateIndexRequest) (*CreateIndexResponse, error) {
	if err := a.eventStore.CreateIndex(ctx, createIndexRequestFromProto(req)); err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return &CreateIndexResponse{}, nil
}

func (a *EventStoreAdapter) DropIndex(ctx context.Context, req *DropIndexRequest) (*DropIndexResponse, error) {
	if err := a.eventStore.DropIndex(ctx, dropIndexRequestFromProto(req)); err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return &DropIndexResponse{}, nil
}

func saveEventsRequestFromProto(req *SaveEventsRequest) *orisun.SaveEventsRequest {
	if req == nil {
		return nil
	}
	result := &orisun.SaveEventsRequest{
		Boundary: req.Boundary,
		Events:   make([]*orisun.EventToSave, len(req.Events)),
	}
	if req.Query != nil {
		result.Query = &orisun.SaveQuery{
			ExpectedPosition: domainPositionFromProto(req.Query.ExpectedPosition),
			SubsetQuery:      domainQueryFromProto(req.Query.SubsetQuery),
		}
	}
	for index, event := range req.Events {
		if event == nil {
			continue
		}
		result.Events[index] = &orisun.EventToSave{
			EventId:   event.EventId,
			EventType: event.EventType,
			Data:      event.Data,
			Metadata:  event.Metadata,
		}
	}
	return result
}

func writeResultToProto(result *orisun.WriteResult) *WriteResult {
	if result == nil {
		return nil
	}
	return &WriteResult{LogPosition: positionToProto(result.LogPosition)}
}

func getEventsRequestFromProto(req *GetEventsRequest) *orisun.GetEventsRequest {
	if req == nil {
		return nil
	}
	direction := orisun.Direction_ASC
	if req.Direction == Direction_DESC {
		direction = orisun.Direction_DESC
	}
	return &orisun.GetEventsRequest{
		Query:        domainQueryFromProto(req.Query),
		FromPosition: domainPositionFromProto(req.FromPosition),
		Count:        req.Count,
		Direction:    direction,
		Boundary:     req.Boundary,
	}
}

func getEventsResponseToProto(response *orisun.GetEventsResponse) *GetEventsResponse {
	if response == nil {
		return nil
	}
	rows := make([]protoEventRow, len(response.Events))
	events := make([]*Event, len(response.Events))
	for index, event := range response.Events {
		fillProtoEventRow(&rows[index], event)
		events[index] = &rows[index].event
	}
	return &GetEventsResponse{Events: events}
}

func latestRequestFromProto(req *GetLatestByCriteriaRequest) *orisun.GetLatestByCriteriaRequest {
	if req == nil {
		return nil
	}
	return &orisun.GetLatestByCriteriaRequest{
		Boundary: req.Boundary,
		Criteria: domainCriteriaFromProto(req.Criteria),
	}
}

func latestResponseToProto(response *orisun.GetLatestByCriteriaResponse) *GetLatestByCriteriaResponse {
	if response == nil {
		return nil
	}
	rows := make([]protoEventRow, len(response.Results))
	results := make([]*LatestCriterionResult, len(response.Results))
	for index, result := range response.Results {
		if result == nil {
			continue
		}
		results[index] = &LatestCriterionResult{
			Criterion: criterionToProto(result.Criterion),
		}
		if result.Event != nil {
			fillProtoEventRow(&rows[index], result.Event)
			results[index].Event = &rows[index].event
		}
	}
	return &GetLatestByCriteriaResponse{
		Results:         results,
		ContextPosition: positionToProto(response.ContextPosition),
	}
}

func createIndexRequestFromProto(req *CreateIndexRequest) *orisun.CreateIndexRequest {
	if req == nil {
		return nil
	}
	result := &orisun.CreateIndexRequest{
		Boundary:   req.Boundary,
		Name:       req.Name,
		Fields:     make([]*orisun.IndexField, len(req.Fields)),
		Conditions: make([]*orisun.IndexCondition, len(req.Conditions)),
	}
	if req.ConditionCombinator == ConditionCombinator_OR {
		result.ConditionCombinator = orisun.ConditionCombinator_OR
	}
	for index, field := range req.Fields {
		if field == nil {
			continue
		}
		result.Fields[index] = &orisun.IndexField{
			JsonKey:   field.JsonKey,
			ValueType: orisun.ValueType(field.ValueType),
		}
	}
	for index, condition := range req.Conditions {
		if condition == nil {
			continue
		}
		result.Conditions[index] = &orisun.IndexCondition{
			Key:      condition.Key,
			Operator: condition.Operator,
			Value:    condition.Value,
		}
	}
	return result
}

func dropIndexRequestFromProto(req *DropIndexRequest) *orisun.DropIndexRequest {
	if req == nil {
		return nil
	}
	return &orisun.DropIndexRequest{Boundary: req.Boundary, Name: req.Name}
}

func domainPositionFromProto(position *Position) *orisun.Position {
	if position == nil {
		return nil
	}
	return &orisun.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func positionToProto(position *orisun.Position) *Position {
	if position == nil {
		return nil
	}
	return &Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func corePositionFromProto(position *Position) *coreeventstore.Position {
	if position == nil {
		return nil
	}
	return &coreeventstore.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func domainQueryFromProto(query *Query) *orisun.Query {
	if query == nil {
		return nil
	}
	return &orisun.Query{Criteria: domainCriteriaFromProto(query.Criteria)}
}

func domainCriteriaFromProto(criteria []*Criterion) []*orisun.Criterion {
	result := make([]*orisun.Criterion, len(criteria))
	for index, criterion := range criteria {
		if criterion == nil {
			continue
		}
		result[index] = &orisun.Criterion{Tags: make([]*orisun.Tag, len(criterion.Tags))}
		for tagIndex, tag := range criterion.Tags {
			if tag == nil {
				continue
			}
			result[index].Tags[tagIndex] = &orisun.Tag{Key: tag.Key, Value: tag.Value}
		}
	}
	return result
}

func coreQueryFromProto(query *Query) coreeventstore.Query {
	if query == nil {
		return coreeventstore.Query{}
	}
	result := coreeventstore.Query{Criteria: make([]coreeventstore.Criterion, len(query.Criteria))}
	for index, criterion := range query.Criteria {
		if criterion == nil {
			continue
		}
		result.Criteria[index].Tags = make([]coreeventstore.Tag, len(criterion.Tags))
		for tagIndex, tag := range criterion.Tags {
			if tag == nil {
				continue
			}
			result.Criteria[index].Tags[tagIndex] = coreeventstore.Tag{Key: tag.Key, Value: tag.Value}
		}
	}
	return result
}

func criterionToProto(criterion *orisun.Criterion) *Criterion {
	if criterion == nil {
		return nil
	}
	result := &Criterion{Tags: make([]*Tag, len(criterion.Tags))}
	for index, tag := range criterion.Tags {
		if tag != nil {
			result.Tags[index] = &Tag{Key: tag.Key, Value: tag.Value}
		}
	}
	return result
}

type protoEventRow struct {
	event     Event
	position  Position
	timestamp timestamppb.Timestamp
}

func fillProtoEventRow(row *protoEventRow, event *orisun.Event) {
	if row == nil || event == nil {
		return
	}
	row.event.EventId = event.EventId
	row.event.EventType = event.EventType
	row.event.Data = event.Data
	row.event.Metadata = event.Metadata
	if event.Position != nil {
		row.position.CommitPosition = event.Position.CommitPosition
		row.position.PreparePosition = event.Position.PreparePosition
		row.event.Position = &row.position
	}
	row.timestamp.Seconds = event.DateCreated.Unix()
	row.timestamp.Nanos = int32(event.DateCreated.Nanosecond())
	row.event.DateCreated = &row.timestamp
}

func readEventToProto(event coreeventstore.ReadEvent) *Event {
	return &Event{
		EventId:   event.EventID,
		EventType: event.EventType,
		Data:      event.Data,
		Metadata:  event.Metadata,
		Position: &Position{
			CommitPosition:  event.Position.CommitPosition,
			PreparePosition: event.Position.PreparePosition,
		},
		DateCreated: timestamppb.New(event.DateCreated),
	}
}
