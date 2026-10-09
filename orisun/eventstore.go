//go:build !orisun_embedded

package orisun

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"

	"runtime/debug"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"

	"github.com/nats-io/nats.go/jetstream"
)

type EventStore struct {
	js                        jetstream.JetStream
	saveEventsFn              EventsSaver
	getEventsFn               EventsRetriever
	lockProvider              LockProvider
	indexManager              BoundaryIndexManager
	logger                    logging.Logger
	subscriptionIdleThreshold time.Duration
	metrics                   atomic.Pointer[eventStoreMetrics]

	boundaryStateMu           sync.RWMutex
	enforceBoundaryActivation bool
	activeBoundaries          map[string]struct{}
}

func NewEventStoreServer(
	js jetstream.JetStream,
	saveEventsFn EventsSaver,
	getEventsFn EventsRetriever,
	lockProvider LockProvider,
	indexManager BoundaryIndexManager,
	logger logging.Logger,
) *EventStore {

	store := &EventStore{
		js:                        js,
		saveEventsFn:              saveEventsFn,
		getEventsFn:               getEventsFn,
		lockProvider:              lockProvider,
		indexManager:              indexManager,
		logger:                    logger,
		activeBoundaries:          make(map[string]struct{}),
		subscriptionIdleThreshold: defaultSubscriptionIdleThreshold,
	}
	return store
}

// EnableBoundaryActivationGate makes public event-store operations require a
// locally observed ACTIVE catalog state. The bootstrap boundary is supplied as
// initially active so the catalog can replay itself before application
// boundaries are exposed.
func (s *EventStore) EnableBoundaryActivationGate(initiallyActive ...string) error {
	if s == nil {
		return fmt.Errorf("event store is not configured")
	}
	for _, boundary := range initiallyActive {
		if err := boundarymodel.ValidateName(boundary); err != nil {
			return fmt.Errorf("invalid active boundary %q: %w", boundary, err)
		}
	}
	s.boundaryStateMu.Lock()
	defer s.boundaryStateMu.Unlock()
	if s.activeBoundaries == nil {
		s.activeBoundaries = make(map[string]struct{}, len(initiallyActive))
	}
	for _, boundary := range initiallyActive {
		s.activeBoundaries[boundary] = struct{}{}
	}
	s.enforceBoundaryActivation = true
	return nil
}

// ActivateBoundary exposes a boundary to public requests after its activation
// event is durable. It is idempotent for replay and clustered delivery.
func (s *EventStore) ActivateBoundary(boundary string) error {
	if s == nil {
		return fmt.Errorf("event store is not configured")
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return fmt.Errorf("invalid active boundary %q: %w", boundary, err)
	}
	s.boundaryStateMu.Lock()
	if s.activeBoundaries == nil {
		s.activeBoundaries = make(map[string]struct{})
	}
	s.activeBoundaries[boundary] = struct{}{}
	s.boundaryStateMu.Unlock()
	return nil
}

// RequireBoundaryActive rejects unknown, provisioning, and failed catalog
// boundaries before a public request reaches a backend.
func (s *EventStore) RequireBoundaryActive(boundary string) error {
	if s == nil {
		return statuscode.New(statuscode.Internal, "event store is not configured")
	}
	s.boundaryStateMu.RLock()
	enforce := s.enforceBoundaryActivation
	_, active := s.activeBoundaries[boundary]
	s.boundaryStateMu.RUnlock()
	if !enforce {
		return nil
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return statuscode.Errorf(statuscode.InvalidArgument, "invalid boundary %q: %v", boundary, err)
	}
	if !active {
		return statuscode.Errorf(statuscode.FailedPrecondition, "boundary %q is not active", boundary)
	}
	return nil
}

// EnsureBoundary validates a boundary. Core NATS subjects require no provisioning.
func (s *EventStore) EnsureBoundary(ctx context.Context, boundary string) error {
	if s == nil || s.js == nil {
		return fmt.Errorf("event store is not configured")
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return fmt.Errorf("invalid boundary %q: %w", boundary, err)
	}
	return ctx.Err()
}

func prepareRequestedEventsForSave(events []*EventToSave) (PreparedEventBatch, error) {
	prepared := make(PreparedEventBatch, len(events))
	for i, event := range events {
		if event == nil {
			return nil, fmt.Errorf("event %d is nil", i)
		}
		dataJSON, err := prepareEventDataJSON(event.Data)
		if err != nil {
			return nil, fmt.Errorf("event %d data: %w", i, err)
		}
		// The gRPC contract historically accepts metadata objects (or null), not
		// arbitrary JSON scalars. Keep that validation at the transport edge.
		metadataJSON, err := prepareJSONObjectJSON(event.Metadata, false)
		if err != nil {
			return nil, fmt.Errorf("event %d metadata: %w", i, err)
		}
		prepared[i] = PreparedEvent{
			EventId:      event.EventId,
			EventType:    event.EventType,
			DataJSON:     dataJSON,
			MetadataJSON: metadataJSON,
		}
	}
	return prepared, nil
}

func authorizeRequest(ctx context.Context, roles []Role) error {
	// Check if the user has the necessary permissions to perform the query
	user := ctx.Value(UserContextKey)
	if user == nil {
		return nil
	}

	// Check if the user has any of the necessary permissions to perform the query
	userObj := user.(User)

	// If no roles are specified, allow access
	if len(roles) == 0 {
		return nil
	}

	// Check if the user has any of the required roles
	for _, requiredRole := range roles {
		if slices.Contains(userObj.Roles, requiredRole) {
			// User has at least one of the required roles
			return nil
		}
	}

	// User doesn't have any of the required roles
	return statuscode.Errorf(statuscode.PermissionDenied, "user does not have any of the required roles")
}

func (s *EventStore) Ping(ctx context.Context) error {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("Ping called")
	}
	return nil
}

func (s *EventStore) CreateIndex(ctx context.Context, req *CreateIndexRequest) error {
	if err := authorizeRequest(ctx, []Role{RoleAdmin}); err != nil {
		return err
	}
	if s.indexManager == nil {
		return statuscode.Errorf(statuscode.Unimplemented, "index management is not configured")
	}
	if req == nil {
		return statuscode.Errorf(statuscode.InvalidArgument, "create index request is required")
	}
	if req.Boundary == "" || req.Name == "" || len(req.Fields) == 0 {
		return statuscode.Errorf(statuscode.InvalidArgument, "boundary, name, and at least one field are required")
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return err
	}

	fields := make([]BoundaryIndexField, len(req.Fields))
	for i, f := range req.Fields {
		if f == nil {
			return statuscode.Errorf(statuscode.InvalidArgument, "field %d is nil", i)
		}
		if f.JsonKey == "" {
			return statuscode.Errorf(statuscode.InvalidArgument, "each field must have a json_key")
		}
		fields[i] = BoundaryIndexField{
			JsonKey:   f.JsonKey,
			ValueType: strings.ToLower(f.ValueType.String()),
		}
	}

	conditions := make([]BoundaryIndexCondition, len(req.Conditions))
	for i, c := range req.Conditions {
		if c == nil {
			return statuscode.Errorf(statuscode.InvalidArgument, "condition %d is nil", i)
		}
		if c.Key == "" {
			return statuscode.Errorf(statuscode.InvalidArgument, "each condition must have a key")
		}
		conditions[i] = BoundaryIndexCondition{
			Key:      c.Key,
			Operator: c.Operator,
			Value:    c.Value,
		}
	}

	combinator := IndexCombinatorAND
	if req.ConditionCombinator == ConditionCombinator_OR {
		combinator = IndexCombinatorOR
	}

	if err := s.indexManager.CreateBoundaryIndex(ctx, req.Boundary, req.Name, fields, conditions, combinator); err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return err
		}
		return statuscode.Errorf(statuscode.Internal, "failed to create index: %v", err)
	}
	return nil
}

func (s *EventStore) DropIndex(ctx context.Context, req *DropIndexRequest) error {
	if err := authorizeRequest(ctx, []Role{RoleAdmin}); err != nil {
		return err
	}
	if s.indexManager == nil {
		return statuscode.Errorf(statuscode.Unimplemented, "index management is not configured")
	}
	if req == nil {
		return statuscode.Errorf(statuscode.InvalidArgument, "drop index request is required")
	}
	if req.Boundary == "" || req.Name == "" {
		return statuscode.Errorf(statuscode.InvalidArgument, "boundary and name are required")
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return err
	}
	if err := s.indexManager.DropBoundaryIndex(ctx, req.Boundary, req.Name); err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return err
		}
		return statuscode.Errorf(statuscode.Internal, "failed to drop index: %v", err)
	}
	return nil
}

func (s *EventStore) ListIndexes(ctx context.Context, req *ListIndexesRequest) (*ListIndexesResponse, error) {
	if err := authorizeRequest(ctx, []Role{RoleAdmin, RoleOperations}); err != nil {
		return nil, err
	}
	if s.indexManager == nil {
		return nil, statuscode.Errorf(statuscode.Unimplemented, "index management is not configured")
	}
	if req == nil || req.Boundary == "" {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "boundary is required")
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	indexes, err := s.indexManager.ListBoundaryIndexes(ctx, req.Boundary)
	if err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return nil, err
		}
		return nil, statuscode.Errorf(statuscode.Internal, "failed to list indexes: %v", err)
	}
	result := make([]*BoundaryIndex, len(indexes))
	for i := range indexes {
		index := indexes[i]
		result[i] = &index
	}
	return &ListIndexesResponse{Indexes: result}, nil
}

func (s *EventStore) GetIndex(ctx context.Context, req *GetIndexRequest) (*GetIndexResponse, error) {
	if err := authorizeRequest(ctx, []Role{RoleAdmin, RoleOperations}); err != nil {
		return nil, err
	}
	if s.indexManager == nil {
		return nil, statuscode.Errorf(statuscode.Unimplemented, "index management is not configured")
	}
	if req == nil || req.Boundary == "" || req.Name == "" {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "boundary and name are required")
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	index, err := s.indexManager.GetBoundaryIndex(ctx, req.Boundary, req.Name)
	if err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return nil, err
		}
		return nil, statuscode.Errorf(statuscode.Internal, "failed to get index: %v", err)
	}
	return &GetIndexResponse{Index: index}, nil
}

func (s *EventStore) SaveEventsV2(ctx context.Context, req *SaveEventsV2Request) (*WriteResult, error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("SaveEventsV2 called with req: %v", req)
	}
	if err := authorizeRequest(ctx, []Role{RoleAdmin, RoleOperations}); err != nil {
		return nil, err
	}
	return s.saveEventsV2Request(ctx, req)
}

func (s *EventStore) saveEventsV2Request(ctx context.Context, req *SaveEventsV2Request) (*WriteResult, error) {
	if err := validateSaveEventsV2Request(req); err != nil {
		return nil, err
	}
	checks, err := ConsistencyChecksFromObservations(req.Consistency)
	if err != nil {
		return nil, err
	}
	return s.saveEvents(ctx, req.Boundary, req.Events, checks)
}

func (s *EventStore) saveEvents(
	ctx context.Context,
	boundary string,
	events []*EventToSave,
	checks []ConsistencyCheck,
) (resp *WriteResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Errorf("panic while saving events: %v\nStack Trace:\n%s", recovered, debug.Stack())
			resp = nil
			err = statuscode.Errorf(statuscode.Internal, "Internal server error")
		}
	}()
	if err = s.RequireBoundaryActive(boundary); err != nil {
		return nil, err
	}

	prepared, prepareErr := prepareRequestedEventsForSave(events)
	if prepareErr != nil {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "invalid event JSON: %v", prepareErr)
	}
	transactionID, globalID, err := s.savePreparedWithMetrics(
		ctx,
		s.saveEventsFn,
		prepared,
		boundary,
		checks,
	)

	if err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return nil, err
		}
		return nil, statuscode.Errorf(statuscode.Internal, "failed to save events: %v", err)
	}

	tranId, err := parseInt64(transactionID)
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "failed to save events: %v", err)
	}

	return &WriteResult{
		WriteId: WriteID(tranId, globalID),
		LogPosition: &Position{
			CommitPosition:  tranId,
			PreparePosition: globalID,
		},
	}, nil
}

func (s *EventStore) GetEvents(ctx context.Context, req *GetEventsRequest) (*GetEventsResponse, error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("GetEvents called with req: %v", req)
	}
	if req == nil {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "get events request is required")
	}
	if req.Count == 0 {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "Count cannot be 0")
	}
	if req.Count > MaxReadBatchSize {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "Count cannot exceed %d", MaxReadBatchSize)
	}
	if err := ValidateQuery(req.Query); err != nil {
		return nil, err
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	// if req.FromPosition != nil && req.Stream != nil {
	// 	return nil, status.Error(codes.InvalidArgument, "fromPosition and stream cannot be set together, you can only set one of both")
	// }
	batch, err := s.getEventsFn.GetBatch(ctx, req)
	if err != nil {
		return nil, err
	}
	return batch.Response(), nil
}

func (s *EventStore) GetLatestByCriteria(ctx context.Context, req *GetLatestByCriteriaRequest) (*GetLatestByCriteriaResponse, error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("GetLatestByCriteria called with req: %v", req)
	}
	if req == nil {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "get latest by criteria request is required")
	}
	if req.Boundary == "" {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "boundary is required")
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	if len(req.Criteria) == 0 {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "at least one criterion is required")
	}
	if err := ValidateQuery(&Query{Criteria: req.Criteria}); err != nil {
		return nil, err
	}
	for i, criterion := range req.Criteria {
		if criterion == nil || len(criterion.Tags) == 0 {
			return nil, statuscode.Errorf(statuscode.InvalidArgument, "criterion %d has no tags", i)
		}
		for j, tag := range criterion.Tags {
			if tag == nil {
				return nil, statuscode.Errorf(statuscode.InvalidArgument, "criterion %d tag %d is nil", i, j)
			}
		}
	}
	batch, err := s.getEventsFn.GetLatestByCriteria(ctx, latestQueryFromRequest(req))
	if err != nil {
		return nil, err
	}
	if len(batch.Matches) != len(req.Criteria) {
		return nil, statuscode.Errorf(statuscode.Internal, "latest result count %d does not match criterion count %d", len(batch.Matches), len(req.Criteria))
	}
	return latestBatchResponse(batch, req.Criteria), nil
}

func latestQueryFromRequest(req *GetLatestByCriteriaRequest) LatestByCriteriaQuery {
	tagCount := 0
	for _, criterion := range req.Criteria {
		tagCount += len(criterion.Tags)
	}
	tags := make([]ReadTag, tagCount)
	criteria := make([]ReadCriterion, len(req.Criteria))
	offset := 0
	for i, criterion := range req.Criteria {
		start := offset
		for _, tag := range criterion.Tags {
			tags[offset] = ReadTag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator}
			offset++
		}
		criteria[i].Tags = tags[start:offset]
	}
	return LatestByCriteriaQuery{Boundary: req.Boundary, Criteria: criteria}
}

func latestBatchResponse(batch LatestByCriteriaBatch, criteria []*Criterion) *GetLatestByCriteriaResponse {
	rows := make([]Event, len(criteria))
	results := make([]LatestCriterionResult, len(criteria))
	pointers := make([]*LatestCriterionResult, len(criteria))
	for i, criterion := range criteria {
		result := &results[i]
		result.Criterion = criterion
		if i < len(batch.Matches) && batch.Matches[i].Found {
			fillEvent(&rows[i], &batch.Matches[i].Event)
			result.Event = &rows[i]
		}
		pointers[i] = result
	}
	return &GetLatestByCriteriaResponse{
		Results: pointers,
		ContextPosition: &Position{
			CommitPosition:  batch.ContextCommitPosition,
			PreparePosition: batch.ContextPreparePosition,
		},
	}
}

func neutralSubscriptionReadEvent(event ReadEvent) coreeventstore.ReadEvent {
	return coreeventstore.ReadEvent{
		EventID:   event.EventId,
		WriteID:   event.WriteId,
		EventType: event.EventType,
		Data:      event.Data,
		Metadata:  event.Metadata,
		Position: coreeventstore.Position{
			CommitPosition:  event.CommitPosition,
			PreparePosition: event.PreparePosition,
		},
		DateCreated: event.DateCreated,
	}
}

func subscriptionPosition(position *coreeventstore.Position) *Position {
	if position == nil {
		return nil
	}
	return &Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func subscriptionQuery(query coreeventstore.Query) *Query {
	if len(query.Criteria) == 0 {
		return nil
	}
	result := &Query{Criteria: make([]*Criterion, len(query.Criteria))}
	for index, criterion := range query.Criteria {
		result.Criteria[index] = &Criterion{Tags: make([]*Tag, len(criterion.Tags))}
		for tagIndex, tag := range criterion.Tags {
			result.Criteria[index].Tags[tagIndex] = &Tag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator}
		}
	}
	return result
}

type ComparationResult int

const IsLessThan ComparationResult = -1
const IsEqual ComparationResult = 0
const IsGreaterThan ComparationResult = 1

func ComparePositions(p1, p2 *Position) ComparationResult {
	if p1.CommitPosition == p2.CommitPosition && p1.PreparePosition == p2.PreparePosition {
		return 0
	}

	if (p1.CommitPosition < p2.CommitPosition) ||
		(p1.CommitPosition == p2.CommitPosition && p1.PreparePosition < p2.PreparePosition) {
		return -1
	}

	return 1
}

// isEventPositionNewerThanPosition checks if the new event position is greater than the last processed position
func isEventPositionNewerThanPosition(newPosition, lastPosition *Position) bool {
	compResult := ComparePositions(newPosition, lastPosition)

	return compResult == IsGreaterThan
}

func validateSaveEventsV2Request(req *SaveEventsV2Request) error {
	if req == nil {
		return statuscode.New(statuscode.InvalidArgument, "Invalid request: missing request body")
	}
	if len(req.Events) == 0 {
		return statuscode.New(statuscode.InvalidArgument, "Invalid request: no events provided")
	}
	return nil
}

func InitializeEventStore(
	ctx context.Context,
	config c.AppConfig,
	saveEvents EventsSaver,
	getEvents EventsRetriever,
	lockProvider LockProvider,
	indexManager BoundaryIndexManager,
	js jetstream.JetStream,
	logger logging.Logger) *EventStore {

	logger.Info("Initializing EventStore")
	eventStore := NewEventStoreServer(
		js,
		saveEvents,
		getEvents,
		lockProvider,
		indexManager,
		logger,
	)
	if err := eventStore.EnsureBoundary(ctx, config.Admin.Boundary); err != nil {
		logger.Fatalf("failed to validate admin boundary %s: %v", config.Admin.Boundary, err)
	}
	eventStore.subscriptionIdleThreshold = config.SubscriptionIdleThreshold
	logger.Info("EventStore initialized")

	return eventStore
}

type Backoff struct {
	Base, Max time.Duration
	cur       time.Duration
}

// Wait sleeps for the current interval (with up to 50% jitter), doubles it for next time
// (capped at Max), and returns ctx.Err if cancelled.
func (b *Backoff) Wait(ctx context.Context) error {
	if b.cur <= 0 {
		b.cur = b.Base
	}
	jitter := time.Duration(rand.Int64N(int64(b.cur)/2 + 1))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(b.cur + jitter):
	}
	b.cur *= 2
	if b.cur > b.Max {
		b.cur = b.Max
	}
	return nil
}

func (b *Backoff) Reset() { b.cur = 0 }

func positionValuesAfter(commit, prepare, previousCommit, previousPrepare int64) bool {
	return commit > previousCommit || (commit == previousCommit && prepare > previousPrepare)
}
