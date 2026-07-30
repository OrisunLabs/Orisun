package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/nats-io/nats.go/jetstream"
)

const insertEventsWithConsistency = `
SELECT * FROM %s.insert_events_with_consistency_v3($1::text, $2::text, $3::jsonb, $4::jsonb)
`

const insertEventRequestsWithConsistency = `
SELECT * FROM %s.insert_event_requests_with_consistency_v1($1::text, $2::text, $3::jsonb)
`

const insertUnconditionalEventRequests = `
SELECT * FROM %s.insert_unconditional_event_requests_v1($1::text, $2::text, $3::jsonb)
`

const insertCanonicalEventRequestsWithConsistency = `
SELECT * FROM %s.insert_canonical_event_requests_with_consistency_v1($1::text, $2::text, $3::jsonb)
`

const insertIndependentEventRequestsWithConsistency = `
SELECT * FROM %s.insert_independent_event_requests_with_consistency_v1($1::text, $2::text, $3::text, $4::jsonb)
`

const selectMatchingEvents = `
SELECT * FROM %s.get_matching_events_v3($1::text, $2::text, $3::jsonb, $4::jsonb, $5, $6::INT)
`

const selectLatestByCriteria = `
SELECT * FROM %s.get_latest_by_criteria_v1($1::text, $2::text, $3::jsonb)
`

const invalidIndexCleanupTimeout = 30 * time.Second

type PostgresSaveEvents struct {
	db       *sql.DB
	logger   logging.Logger
	registry *BoundaryRegistry
	gc       postgresGroupCommit
}

func NewPostgresSaveEvents(
	ctx context.Context,
	db *sql.DB,
	logger logging.Logger,
	boundarySchemaMappings map[string]config.BoundaryToPostgresSchemaMapping) *PostgresSaveEvents {
	return NewPostgresSaveEventsWithRegistry(ctx, db, logger, NewBoundaryRegistry(boundarySchemaMappings))
}

func NewPostgresSaveEventsWithRegistry(ctx context.Context, db *sql.DB, logger logging.Logger, registry *BoundaryRegistry) *PostgresSaveEvents {
	s, _ := newPostgresSaveEventsWithRegistry(ctx, db, logger, registry, config.PostgresGroupCommitConfig{})
	return s
}

func NewPostgresSaveEventsWithConfig(
	ctx context.Context,
	db *sql.DB,
	logger logging.Logger,
	boundarySchemaMappings map[string]config.BoundaryToPostgresSchemaMapping,
	gcCfg config.PostgresGroupCommitConfig,
) (*PostgresSaveEvents, error) {
	return newPostgresSaveEventsWithRegistry(ctx, db, logger, NewBoundaryRegistry(boundarySchemaMappings), gcCfg)
}

func newPostgresSaveEventsWithRegistry(
	ctx context.Context,
	db *sql.DB,
	logger logging.Logger,
	registry *BoundaryRegistry,
	gcCfg config.PostgresGroupCommitConfig,
) (*PostgresSaveEvents, error) {
	gcCfg, err := normalizePostgresGroupCommitConfig(gcCfg)
	if err != nil {
		return nil, err
	}
	s := &PostgresSaveEvents{
		db:       db,
		logger:   logger,
		registry: registry,
		gc:       newPostgresGroupCommit(gcCfg),
	}
	for boundary := range registry.Mappings() {
		s.ensureQueue(boundary)
	}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			s.close()
		}()
	}
	return s, nil
}

func (s *PostgresSaveEvents) Schema(boundary string) (string, error) {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return "", errors.New("No schema found for Boundary " + boundary)
	}
	return entry.mapping.Schema, nil
}

type PostgresGetEvents struct {
	db       *sql.DB
	logger   logging.Logger
	registry *BoundaryRegistry
}

func (s *PostgresGetEvents) Schema(boundary string) (string, error) {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return "", errors.New("No schema found for Boundary " + boundary)
	}
	return entry.mapping.Schema, nil
}

func NewPostgresGetEvents(db *sql.DB, logger logging.Logger,
	boundarySchemaMappings map[string]config.BoundaryToPostgresSchemaMapping) *PostgresGetEvents {
	return NewPostgresGetEventsWithRegistry(db, logger, NewBoundaryRegistry(boundarySchemaMappings))
}

func NewPostgresGetEventsWithRegistry(db *sql.DB, logger logging.Logger, registry *BoundaryRegistry) *PostgresGetEvents {
	return &PostgresGetEvents{
		db:       db,
		logger:   logger,
		registry: registry,
	}
}

func (s *PostgresSaveEvents) SavePrepared(
	ctx context.Context,
	events eventstore.PreparedEventBatch,
	boundary string,
	expectedPosition *eventstore.Position,
	streamConsistencyCondition *eventstore.Query) (transactionID string, globalID int64, err error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("Postgres: Saving events from request: %v", events)
	}

	if len(events) == 0 {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "events cannot be empty")
	}

	if _, ok := s.registry.lookup(boundary); !ok {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "no schema found for boundary: %s", boundary)
	}
	return s.enqueue(ctx, boundary, events, expectedPosition, streamConsistencyCondition)
}

func (s *PostgresGetEvents) GetBatch(ctx context.Context, req *eventstore.GetEventsRequest) (eventstore.ReadEventBatch, error) {
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("Getting events from request: %v", req)
	}

	entry, ok := s.registry.lookup(req.Boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "no schema found for boundary: %s", req.Boundary)
	}
	schema := entry.mapping.Schema
	count := req.Count
	if count == 0 {
		count = eventstore.DefaultReadBatchSize
	} else if count > eventstore.MaxReadBatchSize {
		count = eventstore.MaxReadBatchSize
	}

	var fromPositionMarshaled *[]byte = nil

	if req.FromPosition != nil {
		fromPosition := map[string]int64{
			"transaction_id": req.FromPosition.CommitPosition,
			"global_id":      req.FromPosition.PreparePosition,
		}
		fromPositionJson, err := json.Marshal(fromPosition)
		if err != nil {
			return nil, statuscode.Errorf(statuscode.Internal, "failed to marshal from position: %v", err)
		}
		fromPositionMarshaled = &fromPositionJson
	}

	var paramsJSON *[]byte = nil
	if req.Query != nil && len(req.Query.Criteria) > 0 {
		criteriaList := getCriteriaAsList(req.Query)
		if len(criteriaList) > 0 {
			globalQuery := map[string]any{
				"criteria": criteriaList,
			}
			paramsBytes, err := json.Marshal(globalQuery)
			if err != nil {
				return nil, statuscode.Errorf(statuscode.Internal, "failed to marshal params: %v", err)
			}
			paramsJSON = &paramsBytes
		}
	}

	// s.logger.Debugf("params: %v", paramsJSON)
	// s.logger.Debugf("direction: %v", req.Direction)
	// s.logger.Debugf("count: %v", req.Count)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// _, errr := tx.Exec("SET log_statement = 'all';")
	// if errr != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to set log_statement: %v", err)
	// }

	rows, err := tx.Query(
		entry.selectEvents,
		req.Boundary,
		schema,
		paramsJSON,
		fromPositionMarshaled,
		req.Direction.String(),
		count,
	)
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "failed to execute query: %v", err)
	}
	defer rows.Close()

	batch := make(eventstore.ReadEventBatch, 0, count)

	for rows.Next() {
		var event eventstore.ReadEvent
		if err := rows.Scan(
			&event.CommitPosition,
			&event.PreparePosition,
			&event.EventId,
			&event.EventType,
			&event.Data,
			&event.Metadata,
			&event.DateCreated,
		); err != nil {
			return nil, statuscode.Errorf(statuscode.Internal, "failed to scan row: %v", err)
		}
		batch = append(batch, event)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "error iterating rows: %v", err)
	}

	return batch, nil
}

// GetLatestByCriteria returns the latest event per criterion plus the max
// observed position. The per-criterion lookups run inside one SQL statement
// (get_latest_by_criteria_v1 builds a UNION ALL of LIMIT-1 subqueries), so the
// whole context comes from one snapshot — assembling it from independent
// queries would let an event commit in between with a position below the
// observed maximum, invisible to a scalar expected-position check.
func (s *PostgresGetEvents) GetLatestByCriteria(ctx context.Context, query eventstore.LatestByCriteriaQuery) (eventstore.LatestByCriteriaBatch, error) {
	entry, ok := s.registry.lookup(query.Boundary)
	if !ok {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.InvalidArgument, "no schema found for boundary: %s", query.Boundary)
	}
	schema := entry.mapping.Schema
	if len(query.Criteria) == 0 {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.InvalidArgument, "at least one criterion is required")
	}

	criteriaList := getReadCriteriaAsList(query.Criteria)
	if len(criteriaList) != len(query.Criteria) {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.InvalidArgument, "every criterion needs at least one tag")
	}
	paramsBytes, err := json.Marshal(map[string]any{"criteria": criteriaList})
	if err != nil {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "failed to marshal criteria: %v", err)
	}

	rows, err := s.db.QueryContext(ctx, entry.selectLatest, query.Boundary, schema, paramsBytes)
	if err != nil {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "failed to execute query: %v", err)
	}
	defer rows.Close()

	batch := eventstore.LatestByCriteriaBatch{
		Matches:                make([]eventstore.LatestCriterionMatch, len(query.Criteria)),
		ContextCommitPosition:  -1,
		ContextPreparePosition: -1,
	}
	found := false
	for rows.Next() {
		var (
			idx   int
			event eventstore.ReadEvent
		)
		if err := rows.Scan(
			&idx,
			&event.CommitPosition,
			&event.PreparePosition,
			&event.EventId,
			&event.EventType,
			&event.Data,
			&event.Metadata,
			&event.DateCreated,
		); err != nil {
			return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "failed to scan row: %v", err)
		}
		if idx < 0 || idx >= len(batch.Matches) {
			return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "latest criterion index out of range: %d", idx)
		}
		batch.Matches[idx] = eventstore.LatestCriterionMatch{Event: event, Found: true}
		if !found || event.CommitPosition > batch.ContextCommitPosition ||
			(event.CommitPosition == batch.ContextCommitPosition && event.PreparePosition > batch.ContextPreparePosition) {
			batch.ContextCommitPosition = event.CommitPosition
			batch.ContextPreparePosition = event.PreparePosition
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "error iterating rows: %v", err)
	}
	return batch, nil
}

func getReadCriteriaAsList(criteria []eventstore.ReadCriterion) []map[string]any {
	result := make([]map[string]any, 0, len(criteria))
	for _, criterion := range criteria {
		if len(criterion.Tags) == 0 {
			continue
		}
		anded := make(map[string]any, len(criterion.Tags))
		for _, tag := range criterion.Tags {
			anded[tag.Key] = tag.Value
		}
		result = append(result, anded)
	}
	return result
}

func getStreamSectionAsMap(expectedPosition *eventstore.Position, consistencyCondition *eventstore.Query) map[string]any {
	lastRetrievedPositions := make(map[string]any)

	if expectedPosition != nil {
		lastRetrievedPositions["expected_position"] = map[string]int64{
			"transaction_id": expectedPosition.CommitPosition,
			"global_id":      expectedPosition.PreparePosition,
		}
	}

	if conditions := consistencyCondition; conditions != nil {
		lastRetrievedPositions["criteria"] = getCriteriaAsList(consistencyCondition)
	}

	return lastRetrievedPositions
}

func getCriteriaAsList(query *eventstore.Query) []map[string]any {
	result := make([]map[string]any, 0, len(query.Criteria))
	for _, criterion := range query.Criteria {
		anded := make(map[string]any, len(criterion.Tags))
		for _, tag := range criterion.Tags {
			anded[tag.Key] = tag.Value
		}
		result = append(result, anded)
	}
	return result
}

type PostgresAdminDB struct {
	db            *sql.DB
	logger        logging.Logger
	adminSchema   string
	adminBoundary string
	registry      *BoundaryRegistry

	// Pre-formatted admin queries (fixed schema/boundary)
	qListUsers          string
	qGetProjectorPos    string
	qUpsertProjectorPos string
	qDeleteUser         string
	qGetUserByUsername  string
	qGetUserById        string
	qUpsertUser         string
	qGetUsersCount      string
	qSaveUsersCount     string
}

func NewPostgresAdminDB(db *sql.DB, logger logging.Logger, schema string, boundary string, boundarySchemaMappings map[string]config.BoundaryToPostgresSchemaMapping) *PostgresAdminDB {
	return NewPostgresAdminDBWithRegistry(db, logger, schema, boundary, NewBoundaryRegistry(boundarySchemaMappings))
}

func NewPostgresAdminDBWithRegistry(db *sql.DB, logger logging.Logger, schema string, boundary string, registry *BoundaryRegistry) *PostgresAdminDB {
	return &PostgresAdminDB{
		db:            db,
		logger:        logger,
		adminSchema:   schema,
		adminBoundary: boundary,
		registry:      registry,

		qListUsers:          fmt.Sprintf("SELECT id, name, username, password_hash, roles FROM %s.%s_users ORDER BY id", schema, boundary),
		qGetProjectorPos:    fmt.Sprintf("SELECT COALESCE(commit_position, 0), COALESCE(prepare_position, 0) FROM %s.%s_projector_checkpoint where name = $1", schema, boundary),
		qUpsertProjectorPos: fmt.Sprintf("INSERT INTO %s.%s_projector_checkpoint (id, name, commit_position, prepare_position) VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO UPDATE SET commit_position = $3, prepare_position = $4", schema, boundary),
		qDeleteUser:         fmt.Sprintf("DELETE FROM %s.%s_users WHERE id = $1", schema, boundary),
		qGetUserByUsername:  fmt.Sprintf("SELECT id, name, username, password_hash, roles FROM %s.%s_users where username = $1", schema, boundary),
		qGetUserById:        fmt.Sprintf("SELECT id, name, username, password_hash, roles FROM %s.%s_users where id = $1", schema, boundary),
		qUpsertUser:         fmt.Sprintf("INSERT INTO %s.%s_users (id, name, username, password_hash, roles) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET name = $2, username = $3, password_hash = $4, roles = $5, updated_at = $6", schema, boundary),
		qGetUsersCount:      fmt.Sprintf("SELECT user_count FROM %s.%s_users_count limit 1", schema, boundary),
		qSaveUsersCount:     fmt.Sprintf("INSERT INTO %s.%s_users_count (id, user_count, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET user_count = $2, updated_at = $4", schema, boundary),
	}
}

var userCache = map[string]*eventstore.User{}

func (s *PostgresAdminDB) ListAdminUsers(ctx context.Context) ([]*eventstore.User, error) {
	rows, err := s.db.QueryContext(ctx, s.qListUsers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*eventstore.User
	for rows.Next() {
		var user eventstore.User
		user, err = s.scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}

	return users, nil
}

func (s *PostgresAdminDB) GetProjectorLastPosition(ctx context.Context, projectorName string) (*eventstore.Position, error) {
	var commitPos, preparePos int64
	err := s.db.QueryRowContext(
		ctx,
		s.qGetProjectorPos,
		projectorName,
	).Scan(&commitPos, &preparePos)
	if err == sql.ErrNoRows {
		pos := eventstore.NotExistsPosition()
		return &pos, nil
	}
	if err != nil {
		return nil, err
	}

	return &eventstore.Position{
		CommitPosition:  commitPos,
		PreparePosition: preparePos,
	}, nil
}

func (p *PostgresAdminDB) UpdateProjectorPosition(ctx context.Context, name string, position *eventstore.Position) error {

	id, err := uuid.NewV7()
	if err != nil {
		p.logger.Error("Error generating UUID: %v", err)
		return err
	}

	if _, err := p.db.ExecContext(
		ctx,
		p.qUpsertProjectorPos,
		id.String(),
		name,
		position.CommitPosition,
		position.PreparePosition,
	); err != nil {
		p.logger.Error("Error updating checkpoint: %v", err)
		return err
	}
	return nil
}

func (p *PostgresAdminDB) DeleteUser(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(
		ctx,
		p.qDeleteUser,
		id,
	)

	if err != nil {
		p.logger.Error("Error updating checkpoint: %v", err)
		return err
	}
	return nil
}

func (s *PostgresAdminDB) scanUser(rows *sql.Rows) (eventstore.User, error) {
	var user eventstore.User
	var roles []string
	if err := rows.Scan(&user.Id, &user.Name, &user.Username, &user.HashedPassword, pq.Array(&roles)); err != nil {
		s.logger.Error("Failed to scan user row: %v", err)
		return eventstore.User{}, err
	}

	for _, role := range roles {
		user.Roles = append(user.Roles, eventstore.Role(role))
	}
	return user, nil
}

func (s *PostgresAdminDB) GetUserByUsername(ctx context.Context, username string) (eventstore.User, error) {
	user := userCache[username]
	if user != nil {
		if s.logger.IsDebugEnabled() {
			s.logger.Debug("Fetched from cache")
		}
		return *user, nil
	}

	rows, err := s.db.QueryContext(ctx, s.qGetUserByUsername, username)
	if err != nil {
		s.logger.Infof("User: %v", err)
		return eventstore.User{}, err
	}
	defer rows.Close()

	var userResponse eventstore.User
	if rows.Next() {
		userResponse, err = s.scanUser(rows)
		if err != nil {
			return eventstore.User{}, err
		}
	}

	if userResponse.Id != "" {
		userCache[username] = &userResponse
		return userResponse, nil
	}
	return eventstore.User{}, fmt.Errorf("user not found")
}

func (s *PostgresAdminDB) GetUserById(ctx context.Context, id string) (eventstore.User, error) {
	rows, err := s.db.QueryContext(ctx, s.qGetUserById, id)
	if err != nil {
		if s.logger.IsDebugEnabled() {
			s.logger.Debugf("User by ID: %v", err)
		}
		return eventstore.User{}, err
	}
	defer rows.Close()

	var userResponse eventstore.User
	if rows.Next() {
		userResponse, err = s.scanUser(rows)
		if err != nil {
			return eventstore.User{}, err
		}
	}

	if userResponse.Id != "" {
		return userResponse, nil
	}
	return eventstore.User{}, fmt.Errorf("user not found with id: %s", id)
}

func (s *PostgresAdminDB) UpsertUser(ctx context.Context, user eventstore.User) error {
	roleStrings := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roleStrings[i] = string(role)
	}
	rolesStr := "{" + strings.Join(roleStrings, ",") + "}"

	_, err := s.db.ExecContext(
		ctx,
		s.qUpsertUser,
		user.Id,
		user.Name,
		user.Username,
		user.HashedPassword,
		rolesStr,
		time.Now().UTC(),
	)

	if err != nil {
		s.logger.Error("Error upserting user: %v", err)
		return err
	}

	// Update cache
	userCache[user.Username] = &user
	return nil
}

func (s *PostgresAdminDB) GetUsersCount(ctx context.Context) (uint32, error) {
	rows, err := s.db.QueryContext(ctx, s.qGetUsersCount)
	if err != nil {
		if s.logger.IsDebugEnabled() {
			s.logger.Debugf("User count: %v", err)
		}
		return 0, err
	}
	defer rows.Close()

	var count uint32 = 0
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			s.logger.Error("Failed to scan user count: %v", err)
			return 0, err
		}
	}
	return count, nil
}

func (s *PostgresAdminDB) GetEventsCount(ctx context.Context, boundary string) (int, error) {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return 0, fmt.Errorf("no schema mapping found for boundary: %s", boundary)
	}

	rows, err := s.db.QueryContext(ctx, entry.getEventCount)
	if err != nil {
		if s.logger.IsDebugEnabled() {
			s.logger.Debugf("Event count table query error: %v", err)
		}
		var count int
		err := s.db.QueryRowContext(ctx, entry.fallbackGetEventCount).Scan(&count)
		if err != nil {
			s.logger.Errorf("Error getting events count for boundary %s: %v", boundary, err)
			return 0, err
		}
		return count, nil
	}
	defer rows.Close()

	var count = 0
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			s.logger.Error("Failed to scan event count: %v", err)
			return 0, err
		}
	}
	return count, nil
}

const (
	userCountId  = "0195c053-57e7-7a6d-8e17-a2a695f67d1f"
	eventCountId = "0195c053-57e7-7a6d-8e17-a2a695f67d2f"
)

func (s *PostgresAdminDB) SaveUsersCount(ctx context.Context, users_count uint32) error {
	_, err := s.db.ExecContext(
		ctx,
		s.qSaveUsersCount,
		userCountId,
		int64(users_count),
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {
		s.logger.Error("Error creating user count: %v", err)
		return err
	}

	return nil
}

func (s *PostgresAdminDB) SaveEventCount(ctx context.Context, event_count int, boundary string) error {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return fmt.Errorf("no schema mapping found for boundary: %s", boundary)
	}
	_, err := s.db.ExecContext(
		ctx,
		entry.saveEventCount,
		eventCountId,
		event_count,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {
		s.logger.Error("Error creating event count: %v", err)
		return err
	}

	return nil
}

func (db *PostgresAdminDB) CreateBoundaryIndex(
	ctx context.Context,
	boundary, name string,
	fields []eventstore.BoundaryIndexField,
	conditions []eventstore.BoundaryIndexCondition,
	combinator string,
) error {
	entry, ok := db.registry.lookup(boundary)
	if !ok {
		return fmt.Errorf("unknown boundary: %s", boundary)
	}
	schema := entry.mapping.Schema

	if err := validateBoundaryName(name); err != nil {
		return fmt.Errorf("invalid index name %s: %w", name, err)
	}
	if len(fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}
	if combinator == "" {
		combinator = eventstore.IndexCombinatorAND
	}

	// Build index expression list
	exprs := make([]string, len(fields))
	for i, f := range fields {
		key := pq.QuoteLiteral(f.JsonKey)
		switch f.ValueType {
		case "numeric":
			exprs[i] = "((data->>" + key + ")::numeric)"
		case "boolean":
			exprs[i] = "((data->>" + key + ")::boolean)"
		case "timestamptz":
			exprs[i] = "((data->>" + key + ")::timestamptz)"
		default: // "text"
			exprs[i] = "(data->>" + key + ")"
		}
	}

	// Build WHERE clause
	var whereClause string
	if len(conditions) > 0 {
		validOps := map[string]bool{"=": true, ">": true, "<": true, ">=": true, "<=": true}
		validCombinators := map[string]bool{eventstore.IndexCombinatorAND: true, eventstore.IndexCombinatorOR: true}

		if !validCombinators[combinator] {
			return fmt.Errorf("invalid combinator %q: must be AND or OR", combinator)
		}

		predicates := make([]string, len(conditions))
		for i, c := range conditions {
			if !validOps[c.Operator] {
				return fmt.Errorf("invalid operator %q: must be one of =, >, <, >=, <=", c.Operator)
			}
			predicates[i] = "(data->>" + pq.QuoteLiteral(c.Key) + ") " + c.Operator + " " + pq.QuoteLiteral(c.Value)
		}
		whereClause = " WHERE " + strings.Join(predicates, " "+combinator+" ")
	}

	indexName := pq.QuoteIdentifier(boundary + "_" + name + "_idx")
	physicalIndexName := boundary + "_" + name + "_idx"
	tableName := pq.QuoteIdentifier(boundary + "_orisun_es_event")
	schemaName := pq.QuoteIdentifier(schema)
	metadataTable := pq.QuoteIdentifier(boundary + "_orisun_boundary_index_metadata")

	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("encode index fields: %w", err)
	}
	conditionsJSON, err := json.Marshal(conditions)
	if err != nil {
		return fmt.Errorf("encode index conditions: %w", err)
	}
	upsertMetadata := fmt.Sprintf(
		`INSERT INTO %s.%s (name, fields, conditions, combinator, state, date_created, date_updated)
		 VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, NOW(), NOW())
		 ON CONFLICT (name) DO UPDATE SET
		   fields = EXCLUDED.fields,
		   conditions = EXCLUDED.conditions,
		   combinator = EXCLUDED.combinator,
		   state = EXCLUDED.state,
		   date_updated = NOW()`,
		schemaName,
		metadataTable,
	)
	if _, err := db.db.ExecContext(
		ctx,
		upsertMetadata,
		name,
		string(fieldsJSON),
		string(conditionsJSON),
		combinator,
		eventstore.BoundaryIndexStateBuilding,
	); err != nil {
		return fmt.Errorf("persist building index metadata: %w", err)
	}

	if dropped, err := db.dropInvalidBoundaryIndex(ctx, schema, physicalIndexName); err != nil {
		return fmt.Errorf("remove invalid index before creation: %w", err)
	} else if dropped {
		db.logger.Warnf("Dropped invalid PostgreSQL index %s.%s before retry", schema, physicalIndexName)
	}

	ddl := fmt.Sprintf(
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s.%s USING btree (%s)%s",
		indexName,
		schemaName,
		tableName,
		strings.Join(exprs, ", "),
		whereClause,
	)

	if _, err := db.db.ExecContext(ctx, ddl); err != nil {
		cleanupErr := db.cleanupInvalidBoundaryIndex(ctx, schema, physicalIndexName)
		if cleanupErr != nil {
			return errors.Join(
				fmt.Errorf("create index concurrently: %w", err),
				fmt.Errorf("clean up invalid index: %w", cleanupErr),
			)
		}
		return fmt.Errorf("create index concurrently: %w", err)
	}
	exists, valid, err := db.postgresIndexValidity(ctx, schema, physicalIndexName)
	if err != nil {
		return fmt.Errorf("verify created index: %w", err)
	}
	if !exists || !valid {
		cleanupErr := db.cleanupInvalidBoundaryIndex(ctx, schema, physicalIndexName)
		verificationErr := fmt.Errorf(
			"created index %s.%s is not valid",
			schema,
			physicalIndexName,
		)
		if cleanupErr != nil {
			return errors.Join(
				verificationErr,
				fmt.Errorf("clean up invalid index: %w", cleanupErr),
			)
		}
		return verificationErr
	}
	markReady := fmt.Sprintf(
		"UPDATE %s.%s SET state = $2, date_updated = NOW() WHERE name = $1",
		schemaName,
		metadataTable,
	)
	if _, err := db.db.ExecContext(ctx, markReady, name, eventstore.BoundaryIndexStateReady); err != nil {
		return fmt.Errorf("mark index metadata ready: %w", err)
	}
	return nil
}

func (db *PostgresAdminDB) postgresIndexValidity(
	ctx context.Context,
	schema, indexName string,
) (exists, valid bool, err error) {
	const query = `
SELECT i.indisvalid
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relname = LEFT($2, 63)
`
	if err := db.db.QueryRowContext(ctx, query, schema, indexName).Scan(&valid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, false, nil
		}
		return false, false, err
	}
	return true, valid, nil
}

func (db *PostgresAdminDB) dropInvalidBoundaryIndex(
	ctx context.Context,
	schema, indexName string,
) (bool, error) {
	exists, valid, err := db.postgresIndexValidity(ctx, schema, indexName)
	if err != nil || !exists || valid {
		return false, err
	}
	ddl := fmt.Sprintf(
		"DROP INDEX CONCURRENTLY IF EXISTS %s.%s",
		pq.QuoteIdentifier(schema),
		pq.QuoteIdentifier(indexName),
	)
	if _, err := db.db.ExecContext(ctx, ddl); err != nil {
		return false, err
	}
	return true, nil
}

func (db *PostgresAdminDB) cleanupInvalidBoundaryIndex(
	parent context.Context,
	schema, indexName string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent),
		invalidIndexCleanupTimeout,
	)
	defer cancel()
	dropped, err := db.dropInvalidBoundaryIndex(cleanupCtx, schema, indexName)
	if dropped {
		db.logger.Warnf("Dropped invalid PostgreSQL index %s.%s after failed creation", schema, indexName)
	}
	return err
}

func (db *PostgresAdminDB) DropBoundaryIndex(
	ctx context.Context,
	boundary, name string,
) error {
	entry, ok := db.registry.lookup(boundary)
	if !ok {
		return fmt.Errorf("unknown boundary: %s", boundary)
	}
	schema := entry.mapping.Schema

	if err := validateBoundaryName(name); err != nil {
		return fmt.Errorf("invalid index name %s: %w", name, err)
	}

	indexName := pq.QuoteIdentifier(boundary + "_" + name + "_idx")
	schemaName := pq.QuoteIdentifier(schema)
	metadataTable := pq.QuoteIdentifier(boundary + "_orisun_boundary_index_metadata")

	ddl := fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s.%s", schemaName, indexName)
	if _, err := db.db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	deleteMetadata := fmt.Sprintf("DELETE FROM %s.%s WHERE name = $1", schemaName, metadataTable)
	_, err := db.db.ExecContext(ctx, deleteMetadata, name)
	return err
}

func (db *PostgresAdminDB) ListBoundaryIndexes(ctx context.Context, boundary string) ([]eventstore.BoundaryIndex, error) {
	entry, ok := db.registry.lookup(boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	table := pq.QuoteIdentifier(boundary + "_orisun_boundary_index_metadata")
	query := fmt.Sprintf(
		"SELECT name, fields, conditions, combinator, state FROM %s.%s ORDER BY name",
		pq.QuoteIdentifier(entry.mapping.Schema),
		table,
	)
	rows, err := db.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make([]eventstore.BoundaryIndex, 0)
	for rows.Next() {
		index, err := scanPostgresBoundaryIndex(rows)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (db *PostgresAdminDB) GetBoundaryIndex(ctx context.Context, boundary, name string) (*eventstore.BoundaryIndex, error) {
	entry, ok := db.registry.lookup(boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	if err := validateBoundaryName(name); err != nil {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "invalid index name %s: %v", name, err)
	}
	query := fmt.Sprintf(
		"SELECT name, fields, conditions, combinator, state FROM %s.%s WHERE name = $1",
		pq.QuoteIdentifier(entry.mapping.Schema),
		pq.QuoteIdentifier(boundary+"_orisun_boundary_index_metadata"),
	)
	index, err := scanPostgresBoundaryIndex(db.db.QueryRowContext(ctx, query, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, statuscode.Errorf(statuscode.NotFound, "index %q does not exist in boundary %q", name, boundary)
	}
	if err != nil {
		return nil, err
	}
	return &index, nil
}

type postgresIndexScanner interface {
	Scan(dest ...any) error
}

func scanPostgresBoundaryIndex(row postgresIndexScanner) (eventstore.BoundaryIndex, error) {
	var (
		index          eventstore.BoundaryIndex
		fieldsJSON     []byte
		conditionsJSON []byte
	)
	if err := row.Scan(&index.Name, &fieldsJSON, &conditionsJSON, &index.Combinator, &index.State); err != nil {
		return eventstore.BoundaryIndex{}, err
	}
	if err := json.Unmarshal(fieldsJSON, &index.Fields); err != nil {
		return eventstore.BoundaryIndex{}, fmt.Errorf("decode index %s fields: %w", index.Name, err)
	}
	if err := json.Unmarshal(conditionsJSON, &index.Conditions); err != nil {
		return eventstore.BoundaryIndex{}, fmt.Errorf("decode index %s conditions: %w", index.Name, err)
	}
	if index.State == "" {
		index.State = eventstore.BoundaryIndexStateReady
	}
	return index, nil
}

type DatabaseRuntime struct {
	SaveEvents            eventstore.EventsSaver
	GetEvents             eventstore.EventsRetriever
	LockProvider          eventstore.LockProvider
	AdminDB               common.DB
	EventPublishing       eventstore.EventPublishingTracker
	Listener              *PGNotifyListener
	ProvisionBoundary     func(context.Context, boundarymodel.Definition) error
	InstallBoundary       func(context.Context, boundarymodel.Definition) error
	PreexistingAdminStore bool
}

func InitializePostgresDatabaseRuntime(
	ctx context.Context,
	postgresDBConfig config.PostgresDBConfig,
	adminConfig config.AdminConfig,
	js jetstream.JetStream,
	logger logging.Logger,
) *DatabaseRuntime {
	// Create database connection string
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		postgresDBConfig.Host,
		postgresDBConfig.Port,
		postgresDBConfig.User,
		postgresDBConfig.Password,
		postgresDBConfig.Name,
		postgresDBConfig.SSLMode,
	)

	// Create write database pool (optimized for write operations)
	writeDB, err := sql.Open("pgx", connStr)
	if err != nil {
		logger.Fatalf("Failed to connect to write database: %v", err)
	}

	// Configure write pool - fewer connections, shorter lifetimes for consistency
	writeDB.SetMaxOpenConns(postgresDBConfig.WriteMaxOpenConns)
	writeDB.SetMaxIdleConns(postgresDBConfig.WriteMaxIdleConns)
	writeDB.SetConnMaxIdleTime(postgresDBConfig.WriteConnMaxIdleTime)
	writeDB.SetConnMaxLifetime(postgresDBConfig.WriteConnMaxLifetime)

	// Create read database pool (optimized for read operations)
	readDB, err := sql.Open("pgx", connStr)
	if err != nil {
		logger.Fatalf("Failed to connect to read database: %v", err)
	}

	// Configure read pool - more connections, longer lifetimes for performance
	readDB.SetMaxOpenConns(postgresDBConfig.ReadMaxOpenConns)
	readDB.SetMaxIdleConns(postgresDBConfig.ReadMaxIdleConns)
	readDB.SetConnMaxIdleTime(postgresDBConfig.ReadConnMaxIdleTime)
	readDB.SetConnMaxLifetime(postgresDBConfig.ReadConnMaxLifetime)

	// Create admin database pool (optimized for admin operations)
	adminDBPool, err := sql.Open("pgx", connStr)
	if err != nil {
		logger.Fatalf("Failed to connect to admin database: %v", err)
	}

	// Configure admin pool - moderate connections, longer lifetimes for admin tasks
	adminDBPool.SetMaxOpenConns(postgresDBConfig.AdminMaxOpenConns)
	adminDBPool.SetMaxIdleConns(postgresDBConfig.AdminMaxIdleConns)
	adminDBPool.SetConnMaxIdleTime(postgresDBConfig.AdminConnMaxIdleTime)
	adminDBPool.SetConnMaxLifetime(postgresDBConfig.AdminConnMaxLifetime)

	// Test all connections
	if err = writeDB.PingContext(ctx); err != nil {
		logger.Fatalf("Failed to ping write database: %v", err)
	}
	if err = readDB.PingContext(ctx); err != nil {
		logger.Fatalf("Failed to ping read database: %v", err)
	}
	if err = adminDBPool.PingContext(ctx); err != nil {
		logger.Fatalf("Failed to ping admin database: %v", err)
	}

	logger.Info("Database connections established with separate read/write/admin pools")
	logger.Infof("Write pool: %d max open, %d max idle connections",
		postgresDBConfig.WriteMaxOpenConns, postgresDBConfig.WriteMaxIdleConns)
	logger.Infof("Read pool: %d max open, %d max idle connections",
		postgresDBConfig.ReadMaxOpenConns, postgresDBConfig.ReadMaxIdleConns)
	logger.Infof("AdminConfig pool: %d max open, %d max idle connections",
		postgresDBConfig.AdminMaxOpenConns, postgresDBConfig.AdminMaxIdleConns)

	go func() {
		<-ctx.Done()
		logger.Info("Shutting down database connections")
		writeDB.Close()
		readDB.Close()
		adminDBPool.Close()
	}()

	postgesBoundarySchemaMappings := postgresDBConfig.BootstrapSchemaMapping(adminConfig.Boundary)
	adminSchema := postgesBoundarySchemaMappings[adminConfig.Boundary]
	preexistingAdminStore, err := boundaryEventTableExists(
		ctx,
		writeDB,
		adminSchema.Schema,
		adminConfig.Boundary,
	)
	if err != nil {
		logger.Fatalf("Failed to inspect PostgreSQL admin event store: %v", err)
	}
	catalogNative, err := catalogNativeMarkerExists(
		ctx,
		writeDB,
		adminSchema.Schema,
		adminConfig.Boundary,
	)
	if err != nil {
		logger.Fatalf("Failed to inspect PostgreSQL catalog bootstrap marker: %v", err)
	}
	preexistingAdminStore = preexistingAdminStore && !catalogNative
	dbDialect := postgresDBConfig.DatabaseDialect()

	// Create PG LISTEN/NOTIFY listener if enabled.
	var pgListener *PGNotifyListener
	if postgresDBConfig.ListenEnabled {
		var listenErr error
		pgListener, listenErr = NewPGNotifyListener(ctx, connStr, postgesBoundarySchemaMappings, logger)
		if listenErr != nil {
			logger.Warnf("PG LISTEN disabled (connection failed): %v — falling back to polling", listenErr)
			pgListener = nil
		} else {
			logger.Info("PG LISTEN/NOTIFY listener created")
		}
	} else {
		logger.Info("PG LISTEN disabled by configuration — using polling fallback")
	}

	// First, create all unique schemas
	uniqueSchemas := make(map[string]struct{})
	for _, mapping := range postgesBoundarySchemaMappings {
		uniqueSchemas[mapping.Schema] = struct{}{}
	}
	for schema := range uniqueSchemas {
		if schema != "public" {
			if err := createSchemaIfNotExists(writeDB, schema, ctx); err != nil {
				logger.Fatalf("Failed to create schema %s: %v", schema, err)
			}
		}
	}
	if !preexistingAdminStore && !catalogNative {
		if err := markCatalogNativeInstall(ctx, writeDB, adminSchema.Schema, adminConfig.Boundary); err != nil {
			logger.Fatalf("Failed to record PostgreSQL catalog-native installation: %v", err)
		}
	}

	// Initialize tables for each boundary
	for boundary, mapping := range postgesBoundarySchemaMappings {
		isAdminBoundary := boundary == adminConfig.Boundary
		// Use write pool for database migrations (schema changes)
		if err = RunDbScriptsWithDialect(writeDB, boundary, mapping.Schema, isAdminBoundary, dbDialect, ctx); err != nil {
			logger.Fatalf("Failed to run database migrations for boundary %s in schema %s: %v", boundary, mapping.Schema, err)
		}

		logger.Infof("Database migrations for boundary %s in schema %s completed successfully", boundary, mapping.Schema)
	}

	// Every boundary-aware component shares this registry so a provisioned
	// boundary becomes readable, writable, publishable, and administrable as
	// one atomic in-memory registration.
	boundaryRegistry := NewBoundaryRegistry(postgesBoundarySchemaMappings)

	// Use write pool for save operations and read pool for get operations
	saveEvents, err := newPostgresSaveEventsWithRegistry(
		ctx,
		writeDB,
		logger,
		boundaryRegistry,
		postgresDBConfig.GroupCommit,
	)
	if err != nil {
		logger.Fatalf("Invalid PostgreSQL group commit configuration: %v", err)
	}
	getEvents := NewPostgresGetEventsWithRegistry(readDB, logger, boundaryRegistry)
	lockProvider, err := eventstore.NewJetStreamLockProvider(ctx, js, logger)
	if err != nil {
		logger.Fatalf("Failed to create lock provider: %v", err)
	}
	// lockProvider := postgres.NewPGLockProvider(db, AppLogger)
	// Use admin pool for admin operations (user management)
	adminDB := NewPostgresAdminDBWithRegistry(
		adminDBPool,
		logger,
		adminSchema.Schema,
		adminConfig.Boundary,
		boundaryRegistry,
	)

	// Use write pool for event publishing operations
	eventPublishing := NewPostgresEventPublishingWithRegistry(
		writeDB,
		logger,
		boundaryRegistry,
	)
	provisioner := NewPostgresBoundaryProvisioner(writeDB, boundaryRegistry, dbDialect)
	installBoundary := provisioner.InstallBoundary
	if pgListener != nil {
		installBoundary = func(installCtx context.Context, definition boundarymodel.Definition) error {
			if err := provisioner.InstallBoundary(installCtx, definition); err != nil {
				return err
			}
			if err := pgListener.EnsureBoundary(installCtx, definition.Name); err != nil {
				return fmt.Errorf("register PostgreSQL notifications for boundary %s: %w", definition.Name, err)
			}
			return nil
		}
	}

	return &DatabaseRuntime{
		SaveEvents:            saveEvents,
		GetEvents:             getEvents,
		LockProvider:          lockProvider,
		AdminDB:               adminDB,
		EventPublishing:       eventPublishing,
		Listener:              pgListener,
		ProvisionBoundary:     provisioner.ProvisionBoundary,
		InstallBoundary:       installBoundary,
		PreexistingAdminStore: preexistingAdminStore,
	}
}
