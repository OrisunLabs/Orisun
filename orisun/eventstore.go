//go:build !orisun_embedded

package orisun

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	"github.com/goccy/go-json"
	"github.com/google/uuid"

	"runtime/debug"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"
)

type contextLockLease struct {
	ctx context.Context
}

func (l contextLockLease) Context() context.Context { return l.ctx }
func (l contextLockLease) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return l.ctx.Err()
}
func (l contextLockLease) Release() {}

func acquireLockLease(ctx context.Context, provider LockProvider, lockName string) (LockLease, error) {
	if leaseProvider, ok := provider.(LockLeaseProvider); ok {
		return leaseProvider.AcquireLock(ctx, lockName)
	}
	if err := provider.Lock(ctx, lockName); err != nil {
		return nil, err
	}
	return contextLockLease{ctx: ctx}, nil
}

//type EventstoreDependencies interface {
//	Save(ctx context.Context,
//		events []EventWithMapTags,
//		boundary string,
//		streamName string,
//		expectedPosition *Position,
//		streamSubSet *Query,
//	) (transactionID string, globalID int64, err error)
//
//	Get(ctx context.Context, req *GetEventsRequest) (*GetEventsResponse, error)
//
//	Lock(ctx context.Context, lockName string) error
//}

type EventStore struct {
	js           jetstream.JetStream
	saveEventsFn EventsSaver
	getEventsFn  EventsRetriever
	lockProvider LockProvider
	indexManager BoundaryIndexManager
	logger       logging.Logger
	streamConfig EventStreamConfig
	metrics      atomic.Pointer[eventStoreMetrics]

	boundaryStateMu           sync.RWMutex
	enforceBoundaryActivation bool
	activeBoundaries          map[string]struct{}
}

const (
	eventsStreamPrefix = "ORISUN_EVENTS"
	EventsSubjectName  = "events"
)

func GetEventsNatsJetstreamStreamStreamName(boundary string) string {
	return eventsStreamPrefix + "___" + boundary
}

func GetEventsSubjectName(boundary string) string {
	return GetEventsNatsJetstreamStreamStreamName(boundary) + "." + EventsSubjectName + ".>"
}

func GetEventsStreamSubjectFilterForSubscription(boundary string, stream *string) string {
	subject := GetEventsNatsJetstreamStreamStreamName(boundary) + "." + EventsSubjectName + "."
	if stream != nil {
		return subject + *stream + ".>"
	}

	return subject + ">"
}

func GetEventJetstreamSubjectName(boundary string, position *Position) string {
	return GetEventsNatsJetstreamStreamStreamName(boundary) + "." + EventsSubjectName + "." + GetEventNatsMessageId(int64(position.PreparePosition), int64(position.CommitPosition))
}

type EventStreamConfig struct {
	MaxBytes int64
	MaxMsgs  int64
	MaxAge   time.Duration
}

func NewEventStoreServer(
	js jetstream.JetStream,
	saveEventsFn EventsSaver,
	getEventsFn EventsRetriever,
	lockProvider LockProvider,
	indexManager BoundaryIndexManager,
	streamCfg EventStreamConfig,
	logger logging.Logger,
) *EventStore {
	if streamCfg.MaxAge <= 0 {
		streamCfg.MaxAge = 5 * time.Minute
	}
	if streamCfg.MaxMsgs == 0 {
		streamCfg.MaxMsgs = -1
	}
	if streamCfg.MaxBytes == 0 {
		streamCfg.MaxBytes = -1
	}

	store := &EventStore{
		js:               js,
		saveEventsFn:     saveEventsFn,
		getEventsFn:      getEventsFn,
		lockProvider:     lockProvider,
		indexManager:     indexManager,
		logger:           logger,
		streamConfig:     streamCfg,
		activeBoundaries: make(map[string]struct{}),
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

// EnsureBoundary creates or updates the real-time stream for a boundary. It
// is idempotent so provisioning retries can safely call it after the durable
// backend has already been created.
func (s *EventStore) EnsureBoundary(ctx context.Context, boundary string) error {
	if s == nil || s.js == nil {
		return fmt.Errorf("event store is not configured")
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return fmt.Errorf("invalid boundary %q: %w", boundary, err)
	}
	streamName := GetEventsNatsJetstreamStreamStreamName(boundary)
	info, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: streamName,
		Subjects: []string{
			GetEventsSubjectName(boundary),
		},
		MaxMsgs:  s.streamConfig.MaxMsgs,
		MaxBytes: s.streamConfig.MaxBytes,
		Storage:  jetstream.MemoryStorage,
		MaxAge:   s.streamConfig.MaxAge,
	})
	if err != nil {
		return fmt.Errorf("create boundary stream %s: %w", streamName, err)
	}
	s.logger.Infof("stream info: %v", info)
	return nil
}

func prepareRequestedEventsForSave(events []*EventToSave) (PreparedEventBatch, error) {
	prepared := make(PreparedEventBatch, len(events))
	for i, event := range events {
		if event == nil {
			return nil, fmt.Errorf("event %d is nil", i)
		}
		dataJSON, err := prepareEventDataJSON(event.Data, event.EventType)
		if err != nil {
			return nil, fmt.Errorf("event %d data: %w", i, err)
		}
		// The gRPC contract historically accepts metadata objects (or null), not
		// arbitrary JSON scalars. Keep that validation at the transport edge.
		metadataJSON, err := prepareJSONObjectJSON(event.Metadata, "", false)
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

func (s *EventStore) SaveEvents(ctx context.Context, req *SaveEventsRequest) (resp *WriteResult, err error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("SaveEvents called with req: %v", req)
	}

	err = authorizeRequest(ctx, []Role{RoleAdmin, RoleOperations})
	if err != nil {
		return nil, err
	}
	// Defer a recovery function to catch any panics
	defer func() {
		if r := recover(); r != nil {
			s.logger.Errorf("Panic in SaveEvents: %v\nStack Trace:\n%s", r, debug.Stack())
			err = statuscode.Errorf(statuscode.Internal, "Internal server error")
		}
	}()

	if err = validateSaveEventsRequest(req); err != nil {
		return nil, err
	}
	if err = s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}

	var transactionID string
	var globalID int64

	// Extract ExpectedPosition and SubsetQuery from Query if present
	var expectedPosition *Position
	var subsetQuery *Query
	if req.Query != nil {
		expectedPosition = req.Query.ExpectedPosition
		subsetQuery = req.Query.SubsetQuery
	}

	prepared, prepareErr := prepareRequestedEventsForSave(req.Events)
	if prepareErr != nil {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "invalid event JSON: %v", prepareErr)
	}
	transactionID, globalID, err = s.savePreparedWithMetrics(
		ctx,
		s.saveEventsFn,
		prepared,
		req.Boundary,
		expectedPosition,
		subsetQuery,
	)

	if err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return nil, err
		}
		if strings.Contains(err.Error(), "OptimisticConcurrencyException") {
			return nil, statuscode.Errorf(statuscode.AlreadyExists, "failed to save events: %v", err)
		}
		return nil, statuscode.Errorf(statuscode.Internal, "failed to save events: %v", err)
	}

	tranId, err := parseInt64(transactionID)
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "failed to save events: %v", err)
	}

	return &WriteResult{
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
			tags[offset] = ReadTag{Key: tag.Key, Value: tag.Value}
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

func (s *EventStore) SubscribeToAllEvents(
	ctx context.Context,
	request coreeventstore.SubscribeRequest,
	handler coreeventstore.EventHandler,
) error {
	if handler == nil {
		return statuscode.New(statuscode.InvalidArgument, "event handler is required")
	}
	boundary := request.Boundary
	if err := s.RequireBoundaryActive(boundary); err != nil {
		return err
	}
	subscriberName := request.SubscriberName
	afterPosition := legacySubscriptionPosition(request.AfterPosition)
	query := legacySubscriptionQuery(request.Query)
	subscriptionName := boundary + "__" + subscriberName
	subscriptionCtx, cancelSubscription := context.WithCancel(ctx)
	defer cancelSubscription()

	// Use errgroup for coordinated error handling and cancellation
	g, gCtx := errgroup.WithContext(subscriptionCtx)
	lease, err := acquireLockLease(gCtx, s.lockProvider, subscriptionName)

	if err != nil {
		return statuscode.Errorf(statuscode.AlreadyExists, "failed to acquire lock: %v", err)
	}
	defer lease.Release()
	gCtx = lease.Context()

	// Initialize position tracking
	lastProcessedPosition := afterPosition

	// Phase 1: Catch-up by polling the event store until we're up to date
	s.logger.Info("Starting catch-up phase: polling event store")

	const batchSize = 100
	for {
		if err := lease.Check(gCtx); err != nil {
			return err
		}

		// Get events from the event store starting from our last processed position
		var getEventsReq *GetEventsRequest
		if lastProcessedPosition == nil {
			// Start from the end if no position specified
			getEventsReq = &GetEventsRequest{
				Count:     batchSize,
				Direction: Direction_DESC,
				Boundary:  boundary,
				Query:     query,
			}
		} else {
			// Continue from the last processed position
			getEventsReq = &GetEventsRequest{
				Count:     batchSize,
				Direction: Direction_ASC,
				Boundary:  boundary,
				Query:     query,
				FromPosition: &Position{
					PreparePosition: lastProcessedPosition.PreparePosition,
					CommitPosition:  lastProcessedPosition.CommitPosition,
				},
			}
		}

		batch, err := s.getEventsFn.GetBatch(gCtx, getEventsReq)
		if err != nil {
			return statuscode.Errorf(statuscode.Internal, "failed to get events during catch-up: %v", err)
		}

		// If no more events, we're caught up
		if len(batch) == 0 {
			s.logger.Info("Catch-up phase completed: no more events in event store")
			break
		}

		// Deliver all events in this batch
		for i := range batch {
			if err := lease.Check(gCtx); err != nil {
				return err
			}
			readEvent := &batch[i]

			if lastProcessedPosition == nil || positionValuesAfter(
				readEvent.CommitPosition,
				readEvent.PreparePosition,
				lastProcessedPosition.CommitPosition,
				lastProcessedPosition.PreparePosition,
			) {
				if err := handler(gCtx, neutralSubscriptionReadEvent(*readEvent)); err != nil {
					return statuscode.Errorf(statuscode.Internal, "event handler failed during catch-up: %v", err)
				}
			}

			lastProcessedPosition = &Position{
				CommitPosition:  readEvent.CommitPosition,
				PreparePosition: readEvent.PreparePosition,
			}
		}

		// If we got fewer events than requested, we're caught up
		if len(batch) < batchSize {
			s.logger.Info("Catch-up phase completed: reached end of event store")
			break
		}
	}

	/***
	Capture the time immediately after polling is completed, and set to 10 seconds before
	to make sure no event is ever missed.
	***/
	pollingCompletedTime := time.Now().Add(-10 * time.Second)

	// Phase 2: Subscribe to NATS for live updates
	s.logger.Info("Starting live phase: subscribing to NATS for real-time updates")

	// Determine the time to start NATS subscription from
	var timeToSubscribeFromJetstream time.Time
	if lastProcessedPosition != nil {
		// Start NATS subscription from the time of the last processed event
		// We need to get the event to find its timestamp
		lastEventReq := &GetEventsRequest{
			Count:     1,
			Direction: Direction_DESC,
			Boundary:  boundary,
			FromPosition: &Position{
				PreparePosition: lastProcessedPosition.PreparePosition,
				CommitPosition:  lastProcessedPosition.CommitPosition,
			},
		}

		lastEventBatch, err := s.getEventsFn.GetBatch(gCtx, lastEventReq)
		if err != nil {
			s.logger.Errorf("Failed to get last processed event for timestamp: %v", err)
			return fmt.Errorf("cannot determine NATS subscription start time: failed to retrieve last processed event: %w", err)
		} else if len(lastEventBatch) == 0 {
			s.logger.Warn("No events found after last processed position, using polling completion time for NATS subscription")
			timeToSubscribeFromJetstream = pollingCompletedTime
		} else {
			timeToSubscribeFromJetstream = lastEventBatch[0].DateCreated
			if s.logger.IsDebugEnabled() {
				s.logger.Debugf("Starting NATS subscription from last processed event time: %v", timeToSubscribeFromJetstream)
			}
		}
	} else {
		// No events processed, start from polling completion time
		timeToSubscribeFromJetstream = pollingCompletedTime
	}

	// Set up NATS subscription for live events
	subs, err := s.js.Stream(gCtx, GetEventsNatsJetstreamStreamStreamName(boundary))
	if err != nil {
		return statuscode.Errorf(statuscode.Internal, "failed to get stream: %v", err)
	}

	natsSubscriptionName := subscriptionName + uuid.New().String()

	consumer, err := subs.CreateOrUpdateConsumer(gCtx, jetstream.ConsumerConfig{
		Name:          natsSubscriptionName,
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		AckPolicy:     jetstream.AckNonePolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		OptStartTime:  &timeToSubscribeFromJetstream,
	})

	if err != nil {
		return statuscode.Errorf(statuscode.Internal, "failed to create consumer: %v", err)
	}
	// Consumer cleanup must outlive the cancelled subscription context, but it
	// must not delay shutdown indefinitely.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cleanupCancel()
		if err := subs.DeleteConsumer(cleanupCtx, natsSubscriptionName); err != nil &&
			!errors.Is(err, jetstream.ErrConsumerNotFound) {
			s.logger.Warnf("Failed to delete subscription consumer %s: %v", natsSubscriptionName, err)
		}
	}()

	// Start consuming messages
	msgs, err := consumer.Messages(jetstream.PullMaxMessages(200))
	if err != nil {
		return statuscode.Errorf(statuscode.Internal, "failed to get message iterator: %v", err)
	}
	defer msgs.Stop()

	// Use errgroup to manage the message processing goroutine
	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Errorf("Message processing goroutine panicked: %v\nStack: %s", r, debug.Stack())
			}
		}()

		for {
			if err := lease.Check(gCtx); err != nil {
				s.logger.Info("Message processing stopped due to context cancellation")
				return err
			}
			msg, err := msgs.Next(jetstream.NextContext(gCtx))
			if err != nil {
				if gCtx.Err() != nil {
					s.logger.Info("Context cancelled, stopping message processing")
					return gCtx.Err()
				}
				s.logger.Errorf("Error getting next message: %v", err)
				// Small backoff to avoid tight loop on repeated errors
				return fmt.Errorf("failed to get next message: %w", err)
			}

			var envelope publishedEventEnvelope
			if err := json.Unmarshal(msg.Data(), &envelope); err != nil {
				s.logger.Errorf("Failed to unmarshal event: %v", err)
				return fmt.Errorf("failed to unmarshal event: %w", err)
			}
			event := envelope.event()

			// Only process events newer than our last processed position
			isNewer := false
			if lastProcessedPosition != nil {
				isNewer = isEventPositionNewerThanPosition(event.Position, lastProcessedPosition)
			} else {
				isNewer = true // If no position, all events are considered new
			}

			if isNewer && s.eventMatchesQueryCriteria(&event, query) {
				if err := lease.Check(gCtx); err != nil {
					s.logger.Info("Context cancelled, not sending event")
					return err
				}
				if err := handler(gCtx, neutralPublishedEvent(event)); err != nil {
					s.logger.Errorf("Event handler failed: %v", err)
					return fmt.Errorf("event handler failed: %w", err)
				}

				lastProcessedPosition = event.Position

				if err := msg.Ack(); err != nil {
					s.logger.Errorf("Failed to acknowledge message: %v", err)
				}
			} else {
				msg.Ack() // Acknowledge messages that don't match criteria or are duplicates
			}
		}
	})

	// Wait for all goroutines to complete or for an error to occur
	return g.Wait()
}

type publishedEventEnvelope struct {
	EventId     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	Data        string    `json:"data"`
	Metadata    string    `json:"metadata"`
	Position    *Position `json:"position"`
	DateCreated struct {
		Seconds int64 `json:"seconds"`
		Nanos   int32 `json:"nanos"`
	} `json:"date_created"`
}

func (e publishedEventEnvelope) event() Event {
	return Event{
		EventId:     e.EventId,
		EventType:   e.EventType,
		Data:        e.Data,
		Metadata:    e.Metadata,
		Position:    e.Position,
		DateCreated: time.Unix(e.DateCreated.Seconds, int64(e.DateCreated.Nanos)),
	}
}

func neutralSubscriptionReadEvent(event ReadEvent) coreeventstore.ReadEvent {
	return coreeventstore.ReadEvent{
		EventID:   event.EventId,
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

func neutralPublishedEvent(event Event) coreeventstore.ReadEvent {
	result := coreeventstore.ReadEvent{
		EventID:   event.EventId,
		EventType: event.EventType,
		Data:      event.Data,
		Metadata:  event.Metadata,
	}
	if event.Position != nil {
		result.Position = coreeventstore.Position{
			CommitPosition:  event.Position.CommitPosition,
			PreparePosition: event.Position.PreparePosition,
		}
	}
	result.DateCreated = event.DateCreated
	return result
}

func legacySubscriptionPosition(position *coreeventstore.Position) *Position {
	if position == nil {
		return nil
	}
	return &Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func legacySubscriptionQuery(query coreeventstore.Query) *Query {
	if len(query.Criteria) == 0 {
		return nil
	}
	result := &Query{Criteria: make([]*Criterion, len(query.Criteria))}
	for index, criterion := range query.Criteria {
		result.Criteria[index] = &Criterion{Tags: make([]*Tag, len(criterion.Tags))}
		for tagIndex, tag := range criterion.Tags {
			result.Criteria[index].Tags[tagIndex] = &Tag{Key: tag.Key, Value: tag.Value}
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

// Add the validation function
func validateSaveEventsRequest(req *SaveEventsRequest) error {
	if req == nil {
		return statuscode.New(statuscode.InvalidArgument, "Invalid request: missing request body")
	}

	if len(req.Events) == 0 {
		return statuscode.New(statuscode.InvalidArgument, "Invalid request: no events provided")
	}

	return nil
}

func (s *EventStore) eventMatchesQueryCriteria(event *Event, criteria *Query) bool {
	if criteria == nil || len(criteria.Criteria) == 0 {
		return true
	}

	unmarshaledData := map[string]any{}
	if err := json.Unmarshal([]byte(event.Data), &unmarshaledData); err != nil {
		return false
	}

	// OR across criteria groups; AND within a group
	for _, criteriaGroup := range criteria.Criteria {
		allTagsMatch := true
		for _, criteriaTag := range criteriaGroup.Tags {
			eventTag, ok := unmarshaledData[criteriaTag.Key]
			if !ok || !eventTagEquals(eventTag, criteriaTag.Value) {
				allTagsMatch = false
				break
			}
		}
		if allTagsMatch {
			return true
		}
	}
	return false
}

// eventTagEquals compares a JSON-decoded value against the criteria's string form.
// Avoids fmt.Sprintf reflection on the hot path for the common scalar cases.
func eventTagEquals(v any, target string) bool {
	switch x := v.(type) {
	case string:
		return x == target
	case bool:
		if x {
			return target == "true"
		}
		return target == "false"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64) == target
	case json.Number:
		return string(x) == target
	case nil:
		return target == "" || target == "null"
	default:
		return fmt.Sprintf("%v", v) == target
	}
}

func GetEventNatsMessageId(preparePosition int64, commitPosition int64) string {
	return fmt.Sprintf("%d%d", preparePosition, commitPosition)
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
		EventStreamConfig{
			MaxBytes: config.Nats.EventStreamMaxBytes,
			MaxMsgs:  config.Nats.EventStreamMaxMsgs,
			MaxAge:   config.Nats.EventStreamMaxAge,
		},
		logger,
	)
	if err := eventStore.EnsureBoundary(ctx, config.Admin.Boundary); err != nil {
		logger.Fatalf("failed to initialize admin boundary stream %s: %v", config.Admin.Boundary, err)
	}
	logger.Info("EventStore initialized")

	return eventStore
}

type PollingSignal struct {
	ticker *time.Ticker
}

func NewPollingSignal(interval time.Duration) *PollingSignal {
	return &PollingSignal{ticker: time.NewTicker(interval)}
}

func (s *PollingSignal) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ticker.C:
		return nil
	}
}

func (s *PollingSignal) Stop() {
	s.ticker.Stop()
}

// Backoff implements capped exponential backoff with jitter.
// Use for retry loops to avoid thundering-herd on shared resources (PG advisory locks, etc).
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

type EventPollingManager struct {
	ctx                    context.Context
	batchSize              uint32
	lockProvider           LockProvider
	getEvents              EventsRetriever
	js                     jetstream.JetStream
	eventPublishingTracker EventPublishingTracker
	signalProvider         func(string) EventSignal
	logger                 logging.Logger

	mu      sync.Mutex
	running map[string]struct{}
}

func StartEventPolling(
	ctx context.Context,
	config c.AppConfig,
	lockProvider LockProvider,
	getEvents EventsRetriever,
	js jetstream.JetStream,
	eventPublishingTracker EventPublishingTracker,
	signalProvider func(string) EventSignal,
	logger logging.Logger) *EventPollingManager {
	manager := &EventPollingManager{
		ctx:                    ctx,
		batchSize:              config.PollingPublisher.BatchSize,
		lockProvider:           lockProvider,
		getEvents:              getEvents,
		js:                     js,
		eventPublishingTracker: eventPublishingTracker,
		signalProvider:         signalProvider,
		logger:                 logger,
		running:                make(map[string]struct{}),
	}
	return manager
}

// StartBoundary starts exactly one publishing loop for a boundary in this
// process. Cluster-wide exclusivity remains enforced by the lock lease.
func (m *EventPollingManager) StartBoundary(boundary string) error {
	if m == nil || m.ctx == nil || m.lockProvider == nil || m.getEvents == nil || m.js == nil || m.eventPublishingTracker == nil || m.signalProvider == nil || m.logger == nil {
		return fmt.Errorf("event polling manager is not configured")
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return fmt.Errorf("invalid boundary %q: %w", boundary, err)
	}
	m.mu.Lock()
	if _, exists := m.running[boundary]; exists {
		m.mu.Unlock()
		return nil
	}
	m.running[boundary] = struct{}{}
	m.mu.Unlock()

	go m.runBoundary(boundary)
	return nil
}

func (m *EventPollingManager) runBoundary(boundary string) {
	backoff := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			lease, err := acquireLockLease(m.ctx, m.lockProvider, boundary)
			if err != nil {
				m.logger.Warnf("Failed to acquire lock for boundary %s: %v - will retry", boundary, err)
				if waitErr := backoff.Wait(m.ctx); waitErr != nil {
					return
				}
				continue
			}
			backoff.Reset()
			m.logger.Infof("Successfully acquired polling lock for boundary %v", boundary)

			lockCtx := lease.Context()
			lastPosition, err := m.eventPublishingTracker.GetLastPublishedEventPosition(lockCtx, boundary)
			if err != nil {
				lease.Release()
				m.logger.Errorf("Failed to get last published position for boundary %s: %v", boundary, err)
				if waitErr := backoff.Wait(m.ctx); waitErr != nil {
					return
				}
				continue
			}
			m.logger.Infof("Last published position for boundary %v: %v", boundary, &lastPosition)

			err = publishEventsLoopWithLease(
				lockCtx,
				m.js,
				m.getEvents,
				m.batchSize,
				&lastPosition,
				boundary,
				m.eventPublishingTracker,
				m.signalProvider(boundary),
				lease,
				m.logger,
			)
			lease.Release()

			if err != nil {
				m.logger.Errorf("Polling stopped for boundary %s: %v - will retry", boundary, err)
				if waitErr := backoff.Wait(m.ctx); waitErr != nil {
					return
				}
			}
		}
	}
}

func publishEventsLoop(
	ctx context.Context,
	js jetstream.JetStream,
	eventStore EventsRetriever,
	batchSize uint32,
	lastPosition *Position,
	boundary string,
	db EventPublishingTracker,
	signal EventSignal,
	logger logging.Logger,
) error {
	return publishEventsLoopWithLease(ctx, js, eventStore, batchSize, lastPosition, boundary, db, signal, contextLockLease{ctx: ctx}, logger)
}

func publishEventsLoopWithLease(
	ctx context.Context,
	js jetstream.JetStream,
	eventStore EventsRetriever,
	batchSize uint32,
	lastPosition *Position,
	boundary string,
	db EventPublishingTracker,
	signal EventSignal,
	lease LockLease,
	logger logging.Logger,
) error {
	const readRetryBase = 100 * time.Millisecond
	const readRetryMax = 2 * time.Second
	const publishRetryBase = 100 * time.Millisecond
	const publishRetryMax = 2 * time.Second

	defer signal.Stop()
	readBackoff := readRetryBase
	cursor := &Position{
		CommitPosition:  lastPosition.CommitPosition,
		PreparePosition: lastPosition.PreparePosition,
	}

	for {
		if err := lease.Check(ctx); err != nil {
			return err
		}

		// Drain all pending events from DB in order. We drain BEFORE waiting on
		// the signal so events already persisted at startup (or committed while
		// the publisher was down) are published immediately rather than waiting
		// for the next NOTIFY / catch-up tick.
		for {
			if err := lease.Check(ctx); err != nil {
				return err
			}

			req := &GetEventsRequest{
				FromPosition: &Position{
					CommitPosition:  cursor.CommitPosition,
					PreparePosition: cursor.PreparePosition + 1,
				},
				Count:     batchSize,
				Direction: Direction_ASC,
				Boundary:  boundary,
			}
			batch, err := eventStore.GetBatch(ctx, req)
			if err != nil {
				logger.Errorf("Failed to get events for boundary %s: %v", boundary, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(readBackoff):
				}
				readBackoff *= 2
				if readBackoff > readRetryMax {
					readBackoff = readRetryMax
				}
				continue
			}
			readBackoff = readRetryBase

			if len(batch) == 0 {
				break
			}

			if err := validatePublishBatch(batch, cursor); err != nil {
				return err
			}

			batchCursor := &Position{
				CommitPosition:  cursor.CommitPosition,
				PreparePosition: cursor.PreparePosition,
			}
			for i := range batch {
				event := &batch[i]
				if err := lease.Check(ctx); err != nil {
					return err
				}
				if !positionValuesAfter(event.CommitPosition, event.PreparePosition,
					batchCursor.CommitPosition, batchCursor.PreparePosition) {
					return fmt.Errorf(
						"event %s position (%d, %d) is not after cursor (%d, %d)",
						event.EventId,
						event.CommitPosition,
						event.PreparePosition,
						batchCursor.CommitPosition,
						batchCursor.PreparePosition,
					)
				}

				subjectName := GetEventJetstreamSubjectName(
					boundary,
					&Position{
						CommitPosition:  event.CommitPosition,
						PreparePosition: event.PreparePosition,
					},
				)
				eventData, err := event.MarshalJSON()
				if err != nil {
					return fmt.Errorf("marshal event: %w", err)
				}

				publishBackoff := publishRetryBase
				for {
					if err := lease.Check(ctx); err != nil {
						return err
					}
					_, err = js.Publish(
						ctx,
						subjectName,
						eventData,
						jetstream.WithMsgID(GetEventNatsMessageId(event.PreparePosition, event.CommitPosition)),
						jetstream.WithRetryAttempts(5),
					)
					if err == nil {
						break
					}
					logger.Errorf("Failed to publish event to NATS for boundary %s: %v (retrying)", boundary, err)
					select {
					case <-ctx.Done():
						return fmt.Errorf("publish event: context cancelled: %w", ctx.Err())
					case <-time.After(publishBackoff):
					}
					publishBackoff *= 2
					if publishBackoff > publishRetryMax {
						publishBackoff = publishRetryMax
					}
				}

				batchCursor.CommitPosition = event.CommitPosition
				batchCursor.PreparePosition = event.PreparePosition
			}

			// The whole batch is now a contiguous acknowledged prefix. Persist only
			// its final position. A crash or lease loss before this write replays the
			// batch, which is safe under the documented at-least-once contract.
			if err := lease.Check(ctx); err != nil {
				return err
			}
			if err := db.InsertLastPublishedEvent(
				ctx,
				boundary,
				batchCursor.CommitPosition,
				batchCursor.PreparePosition,
			); err != nil {
				return fmt.Errorf("insert last published batch checkpoint: %w", err)
			}
			cursor = batchCursor
		}

		// Wait for the next signal (NOTIFY, catch-up tick, or poll) before
		// draining again.
		if err := signal.Wait(ctx); err != nil {
			return err
		}
	}
}

func positionValuesAfter(commit, prepare, previousCommit, previousPrepare int64) bool {
	return commit > previousCommit || (commit == previousCommit && prepare > previousPrepare)
}

func validatePublishBatch(events ReadEventBatch, cursor *Position) error {
	previousCommit := cursor.CommitPosition
	previousPrepare := cursor.PreparePosition
	for i := range events {
		event := &events[i]
		if !positionValuesAfter(event.CommitPosition, event.PreparePosition, previousCommit, previousPrepare) {
			return fmt.Errorf(
				"event %s position (%d, %d) is not after cursor (%d, %d)",
				event.EventId,
				event.CommitPosition,
				event.PreparePosition,
				previousCommit,
				previousPrepare,
			)
		}
		previousCommit = event.CommitPosition
		previousPrepare = event.PreparePosition
	}
	return nil
}
