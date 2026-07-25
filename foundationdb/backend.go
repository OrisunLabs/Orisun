//go:build foundationdb

package foundationdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

const (
	defaultAPIVersion = 730
	defaultRoot       = "orisun"
	backfillChunkSize = 1000
	scanChunkSize     = 1000
	// maxBatchSize bounds a single Save: the 2-byte versionstamp user version
	// distinguishes events within one commit, so offsets must fit in uint16.
	maxBatchSize = 1 << 16
	// maxTransactionBytes keeps a Save under FoundationDB's hard 10MB
	// transaction limit. The estimate counts event payloads plus per-event
	// key/JSON/index overhead; the headroom absorbs index entries and the
	// signal write so an oversized batch fails fast with InvalidArgument
	// instead of an opaque commit-time transaction_too_large error.
	maxTransactionBytes = 9 << 20
)

var (
	apiOnce sync.Once
	apiErr  error
)

type Backend struct {
	db            fdb.Database
	root          string
	adminBoundary string
	boundaryMu    sync.RWMutex
	boundaries    map[string]struct{}
	logger        logging.Logger
	userCacheMu   sync.RWMutex
	userCache     map[string]*eventstore.User
}

func InitializeFoundationDBRuntime(
	ctx context.Context,
	fdbCfg config.FoundationDBConfig,
	adminCfg config.AdminConfig,
	logger logging.Logger,
) (*DatabaseRuntime, error) {
	if fdbCfg.APIVersion == 0 {
		fdbCfg.APIVersion = defaultAPIVersion
	}
	if fdbCfg.Root == "" {
		fdbCfg.Root = defaultRoot
	}
	// 0 = default 10s; negative = explicitly disabled.
	if fdbCfg.TransactionTimeoutMs == 0 {
		fdbCfg.TransactionTimeoutMs = 10000
	}
	if err := ensureAPIVersion(fdbCfg.APIVersion); err != nil {
		return nil, err
	}

	db, err := fdb.OpenDatabase(fdbCfg.ClusterFile)
	if err != nil {
		return nil, err
	}

	// Bound every transaction (including internal retries) so a partitioned or
	// unreachable cluster surfaces as an error instead of a hung gRPC call. FDB's
	// default is no timeout.
	if fdbCfg.TransactionTimeoutMs > 0 {
		if err := db.Options().SetTransactionTimeout(int64(fdbCfg.TransactionTimeoutMs)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set transaction timeout: %w", err)
		}
	}
	if fdbCfg.TransactionRetryLimit > 0 {
		if err := db.Options().SetTransactionRetryLimit(int64(fdbCfg.TransactionRetryLimit)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set transaction retry limit: %w", err)
		}
	}

	backend := &Backend{
		db:            db,
		root:          fdbCfg.Root,
		adminBoundary: adminCfg.Boundary,
		boundaries:    make(map[string]struct{}, 1),
		logger:        logger,
		userCache:     make(map[string]*eventstore.User),
	}
	backend.boundaries[adminCfg.Boundary] = struct{}{}
	if err := backend.ensureBoundaryMarker(ctx, adminCfg.Boundary); err != nil {
		db.Close()
		return nil, fmt.Errorf("bootstrap admin boundary: %w", err)
	}

	// With criteria reads fail-closed, Orisun's own admin slices and the auth
	// user projector cannot run until their criteria shapes are covered.
	if err := backend.ensureSystemIndexes(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure system indexes: %w", err)
	}

	// FoundationDB-native lease lock: durable and crash-failover capable, unlike
	// the in-memory NATS lock the other shared path uses.
	lockProvider := newFDBLockProvider(db, fdbCfg.Root, logger)

	signalProvider := func(boundary string) eventstore.EventSignal {
		return &fdbSignal{
			db:       db,
			key:      backend.signalKey(boundary),
			fallback: time.Second,
			stopped:  make(chan struct{}),
		}
	}
	closeFn := func(context.Context) {
		db.Close()
	}
	return &DatabaseRuntime{
		SaveEvents: backend, GetEvents: backend, LockProvider: lockProvider,
		AdminDB: backend, EventPublishing: backend, SignalProvider: signalProvider,
		ProvisionBoundary: backend.ProvisionBoundary,
		InstallBoundary:   backend.InstallBoundary,
		Close:             closeFn,
	}, nil
}

func ensureAPIVersion(version int) error {
	apiOnce.Do(func() {
		apiErr = fdb.APIVersion(version)
	})
	return apiErr
}

// systemAdminIndexes cover the criteria shapes Orisun's own admin slices and
// auth and boundary slices query on the admin boundary ({eventType, username},
// {eventType, user_id}, {eventType, userId}, {eventType, boundary}). Criteria
// reads are fail-closed, so these must be ready before the admin service starts.
var systemAdminIndexes = []struct {
	name   string
	fields []eventstore.BoundaryIndexField
}{
	{"sys_admin_event_type", []eventstore.BoundaryIndexField{
		{JsonKey: "eventType", ValueType: "text"},
	}},
	{"sys_admin_et_username", []eventstore.BoundaryIndexField{
		{JsonKey: "eventType", ValueType: "text"},
		{JsonKey: "username", ValueType: "text"},
	}},
	{"sys_admin_et_user_id", []eventstore.BoundaryIndexField{
		{JsonKey: "eventType", ValueType: "text"},
		{JsonKey: "user_id", ValueType: "text"},
	}},
	{"sys_admin_et_userid", []eventstore.BoundaryIndexField{
		{JsonKey: "eventType", ValueType: "text"},
		{JsonKey: "userId", ValueType: "text"},
	}},
	{"sys_admin_et_boundary", []eventstore.BoundaryIndexField{
		{JsonKey: "eventType", ValueType: "text"},
		{JsonKey: "boundary", ValueType: "text"},
	}},
}

func (b *Backend) ensureSystemIndexes(ctx context.Context) error {
	for _, sys := range systemAdminIndexes {
		ready, err := b.indexReadyWithFields(b.adminBoundary, sys.name, sys.fields)
		if err != nil {
			return err
		}
		if ready {
			continue
		}
		if err := b.CreateBoundaryIndex(ctx, b.adminBoundary, sys.name, sys.fields, nil, eventstore.IndexCombinatorAND); err != nil {
			return fmt.Errorf("create system index %s: %w", sys.name, err)
		}
	}
	return nil
}

// indexReadyWithFields reports whether the named index already exists with
// exactly the wanted unconditioned field list — so startup skips a pointless
// clear-and-backfill on every boot.
func (b *Backend) indexReadyWithFields(boundary, name string, fields []eventstore.BoundaryIndexField) (bool, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(b.indexMetaKey(boundary, name)).MustGet()
		if raw == nil {
			return false, nil
		}
		var def indexDefinition
		if err := json.Unmarshal(raw, &def); err != nil {
			return false, nil
		}
		if def.State != indexStateReady || len(def.Conditions) != 0 || len(def.Fields) != len(fields) {
			return false, nil
		}
		for i := range fields {
			if def.Fields[i] != fields[i] {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

func (b *Backend) SavePrepared(
	ctx context.Context,
	events eventstore.PreparedEventBatch,
	boundary string,
	expectedPosition *eventstore.Position,
	streamConsistencyCondition *eventstore.Query,
) (transactionID string, globalID int64, err error) {
	if err := contextStatusErr(ctx); err != nil {
		return "", 0, err
	}
	if len(events) == 0 {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "events cannot be empty")
	}
	if err := b.checkBoundary(boundary); err != nil {
		return "", 0, err
	}
	prepared, err := prepareEvents(events)
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid event JSON: %v", err)
	}

	if len(prepared) > maxBatchSize {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "batch of %d events exceeds max %d", len(prepared), maxBatchSize)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// vsFuture is reassigned on every (re)attempt; after Transact returns it
	// refers to the committed attempt's versionstamp.
	var vsFuture fdb.FutureKey
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		// Parity with the PostgreSQL and SQLite backends: the consistency check
		// runs only when the condition carries criteria. An expected position
		// without criteria is ignored — there is no context to compare against.
		if hasCriteria(streamConsistencyCondition) {
			actualTx, actualGid, err := b.latestMatchingPosition(tr, boundary, streamConsistencyCondition)
			if err != nil {
				return nil, err
			}
			expectedTx, expectedGid := int64(-1), int64(-1)
			if expectedPosition != nil {
				expectedTx, expectedGid = expectedPosition.CommitPosition, expectedPosition.PreparePosition
			}
			if actualTx != expectedTx || actualGid != expectedGid {
				return nil, optimisticConflict(expectedTx, expectedGid, actualTx, actualGid)
			}
		}

		// Index create/drop writes this key. Reading it forces Saves that began
		// before an index metadata change to retry, so they cannot commit
		// without maintaining the current index set.
		_ = tr.Get(b.indexEpochKey(boundary)).MustGet()
		indexes, err := b.loadIndexes(tr, boundary)
		if err != nil {
			return nil, err
		}
		if total := estimateSaveBytes(prepared, indexes); total > maxTransactionBytes {
			return nil, statuscode.Errorf(statuscode.InvalidArgument,
				"batch of %d events is ~%d bytes, exceeding the %d-byte FoundationDB transaction budget; split it",
				len(prepared), total, maxTransactionBytes)
		}
		// Positions are assigned by FDB at commit via versionstamps — no counter
		// read, so concurrent appends to this boundary commit in parallel. Every
		// event (and its index entries) in this batch shares the commit version
		// and is ordered by its user version (the batch offset).
		for i, e := range prepared {
			userVersion := uint16(i)
			e.record.DateCreated = now
			value, err := json.Marshal(e.record)
			if err != nil {
				return nil, err
			}
			eventKey, err := b.eventVersionstampKey(boundary, userVersion)
			if err != nil {
				return nil, err
			}
			tr.SetVersionstampedKey(eventKey, value)
			for _, idx := range indexes {
				if !eventMatchesIndexConditions(e.data, idx) {
					continue
				}
				indexKey, ok, err := b.indexVersionstampKey(boundary, idx, e.data, userVersion)
				if err != nil {
					return nil, err
				}
				if ok {
					tr.SetVersionstampedKey(indexKey, []byte{})
				}
			}
		}
		// Plain Set never creates a write conflict, so the wake-up signal does not
		// serialise writers.
		tr.Set(b.signalKey(boundary), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		vsFuture = tr.GetVersionstamp()
		return nil, nil
	})
	if err != nil {
		if isOptimisticConflict(err) {
			return "", 0, statuscode.New(statuscode.AlreadyExists, err.Error())
		}
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return "", 0, err
		}
		if isTransactionTooLarge(err) {
			return "", 0, statuscode.Errorf(statuscode.InvalidArgument,
				"batch exceeds FoundationDB transaction size limit; split it: %v", err)
		}
		return "", 0, statuscode.Errorf(statuscode.Internal, "save events: %v", err)
	}

	stamp, err := vsFuture.Get()
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.Internal, "resolve versionstamp: %v", err)
	}
	commit := int64(binary.BigEndian.Uint64(stamp[0:8]))
	batch := int64(binary.BigEndian.Uint16(stamp[8:10]))
	lastPrepare := batch<<16 | int64(len(prepared)-1)
	return strconv.FormatInt(commit, 10), lastPrepare, nil
}

func (b *Backend) GetBatch(ctx context.Context, req *eventstore.GetEventsRequest) (eventstore.ReadEventBatch, error) {
	if err := contextStatusErr(ctx); err != nil {
		return nil, err
	}
	if err := b.checkBoundary(req.Boundary); err != nil {
		return nil, err
	}
	count := req.Count
	if count == 0 {
		count = eventstore.DefaultReadBatchSize
	} else if count > eventstore.MaxReadBatchSize {
		count = eventstore.MaxReadBatchSize
	}
	req = &eventstore.GetEventsRequest{
		Query:        req.Query,
		FromPosition: req.FromPosition,
		Count:        count,
		Direction:    req.Direction,
		Boundary:     req.Boundary,
	}
	if hasCriteria(req.Query) {
		events, err := b.query(ctx, req)
		if err != nil {
			if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
				return nil, err
			}
			return nil, statuscode.Errorf(statuscode.Internal, "query events: %v", err)
		}
		return events, nil
	}
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		return b.scanEvents(ctx, rt, req, int(req.Count))
	})
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "query events: %v", err)
	}
	return result.(eventstore.ReadEventBatch), nil
}

// GetLatestByCriteria returns the latest event per criterion plus the max
// observed position, all from ONE FDB read transaction (one read version =
// one snapshot). Like every FDB criteria read, each criterion needs a ready
// covering index — the newest entry of the criterion's index slice IS the
// latest match, so a criterion costs one reverse Limit-1 range read plus one
// event get. Because FDB positions are commit-ordered, no later commit can
// place an event below the returned context position.
func (b *Backend) GetLatestByCriteria(ctx context.Context, query eventstore.LatestByCriteriaQuery) (eventstore.LatestByCriteriaBatch, error) {
	if err := contextStatusErr(ctx); err != nil {
		return eventstore.LatestByCriteriaBatch{}, err
	}
	if err := b.checkBoundary(query.Boundary); err != nil {
		return eventstore.LatestByCriteriaBatch{}, err
	}
	if len(query.Criteria) == 0 {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.InvalidArgument, "at least one criterion is required")
	}
	criteria := readCriteriaAsMaps(query.Criteria)
	if len(criteria) != len(query.Criteria) {
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.InvalidArgument, "every criterion needs at least one tag")
	}

	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		indexes, err := b.loadIndexes(rt, query.Boundary)
		if err != nil {
			return nil, err
		}
		indexes = readyIndexes(indexes)
		batch := eventstore.LatestByCriteriaBatch{
			Matches:                make([]eventstore.LatestCriterionMatch, len(criteria)),
			ContextCommitPosition:  -1,
			ContextPreparePosition: -1,
		}
		found := false
		for i, criterion := range criteria {
			idx, ok := chooseCoveringIndex(indexes, criterion)
			if !ok {
				return nil, b.unindexedQueryErr(query.Boundary, criterion)
			}
			slice := prefixRange(b.indexLookupPrefix(query.Boundary, idx, criterion))
			iter := rt.GetRange(slice, fdb.RangeOptions{
				Limit:   1,
				Mode:    fdb.StreamingModeWantAll,
				Reverse: true,
			}).Iterator()
			for iter.Advance() {
				kv, err := iter.Get()
				if err != nil {
					return nil, err
				}
				tx, gid, err := indexPositionFromKey(kv.Key)
				if err != nil {
					return nil, err
				}
				event, ok, err := b.getReadEventByPosition(rt, query.Boundary, tx, gid)
				if err != nil {
					return nil, err
				}
				if ok {
					batch.Matches[i] = eventstore.LatestCriterionMatch{Event: event, Found: true}
					if !found || event.CommitPosition > batch.ContextCommitPosition ||
						(event.CommitPosition == batch.ContextCommitPosition && event.PreparePosition > batch.ContextPreparePosition) {
						batch.ContextCommitPosition = event.CommitPosition
						batch.ContextPreparePosition = event.PreparePosition
						found = true
					}
				}
			}
		}
		return batch, nil
	})
	if err != nil {
		if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
			return eventstore.LatestByCriteriaBatch{}, err
		}
		return eventstore.LatestByCriteriaBatch{}, statuscode.Errorf(statuscode.Internal, "get latest by criteria: %v", err)
	}
	return result.(eventstore.LatestByCriteriaBatch), nil
}

func (b *Backend) GetLastPublishedEventPosition(ctx context.Context, boundary string) (eventstore.Position, error) {
	if err := b.checkBoundary(boundary); err != nil {
		return eventstore.Position{}, err
	}
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(b.lastPublishedKey(boundary)).MustGet()
		if raw == nil {
			return storedPosition{Commit: -1, Prepare: -1}, nil
		}
		var pos storedPosition
		if err := json.Unmarshal(raw, &pos); err != nil {
			return storedPosition{}, err
		}
		return pos, nil
	})
	if err != nil {
		return eventstore.Position{}, err
	}
	pos := result.(storedPosition)
	return eventstore.Position{CommitPosition: pos.Commit, PreparePosition: pos.Prepare}, nil
}

func (b *Backend) InsertLastPublishedEvent(ctx context.Context, boundary string, transactionID, globalID int64) error {
	if err := b.checkBoundary(boundary); err != nil {
		return err
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		value, err := json.Marshal(storedPosition{Commit: transactionID, Prepare: globalID})
		if err != nil {
			return nil, err
		}
		tr.Set(b.lastPublishedKey(boundary), value)
		return nil, nil
	})
	return err
}

// storedPosition is the JSON checkpoint format — same field names as the
// Position proto's json tags, without copying the proto struct (vet: copylocks).
type storedPosition struct {
	Commit  int64 `json:"commit_position"`
	Prepare int64 `json:"prepare_position"`
}

func (b *Backend) ListAdminUsers(ctx context.Context) ([]*eventstore.User, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		iter := rt.GetRange(prefixRange(b.adminUserByIDPrefix()), fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
		var users []*eventstore.User
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			user, err := decodeUser(kv.Value)
			if err != nil {
				return nil, err
			}
			users = append(users, &user)
		}
		return users, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]*eventstore.User), nil
}

func (b *Backend) GetProjectorLastPosition(ctx context.Context, projectorName string) (*eventstore.Position, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(b.projectorKey(projectorName)).MustGet()
		if raw == nil {
			pos := eventstore.NotExistsPosition()
			return &pos, nil
		}
		var pos eventstore.Position
		if err := json.Unmarshal(raw, &pos); err != nil {
			return nil, err
		}
		return &pos, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*eventstore.Position), nil
}

func (b *Backend) UpdateProjectorPosition(ctx context.Context, name string, position *eventstore.Position) error {
	if position == nil {
		position = &eventstore.Position{}
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		value, err := json.Marshal(position)
		if err != nil {
			return nil, err
		}
		tr.Set(b.projectorKey(name), value)
		return nil, nil
	})
	return err
}

func (b *Backend) UpsertUser(ctx context.Context, user eventstore.User) error {
	if user.Id == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		user.Id = id.String()
	}
	value, err := encodeUser(user)
	if err != nil {
		return err
	}
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if raw := tr.Get(b.adminUserByUsernameKey(user.Username)).MustGet(); raw != nil {
			existing, err := decodeUser(raw)
			if err != nil {
				return nil, err
			}
			if existing.Id != user.Id {
				return nil, statuscode.Errorf(statuscode.AlreadyExists, "username %q already exists", user.Username)
			}
		}
		// A username change must clear the old username key, or the stale
		// mapping would keep serving the old login name forever.
		if raw := tr.Get(b.adminUserByIDKey(user.Id)).MustGet(); raw != nil {
			existing, err := decodeUser(raw)
			if err != nil {
				return nil, err
			}
			if existing.Username != user.Username {
				tr.Clear(b.adminUserByUsernameKey(existing.Username))
			}
		}
		tr.Set(b.adminUserByIDKey(user.Id), value)
		tr.Set(b.adminUserByUsernameKey(user.Username), value)
		return nil, nil
	})
	if err != nil {
		return err
	}
	b.userCacheMu.Lock()
	defer b.userCacheMu.Unlock()
	// Reset the whole cache: a username change would otherwise leave the old
	// username's entry serving stale credentials until restart.
	b.userCache = make(map[string]*eventstore.User)
	b.userCache[user.Username] = &user
	return nil
}

func (b *Backend) DeleteUser(ctx context.Context, id string) error {
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw := tr.Get(b.adminUserByIDKey(id)).MustGet()
		if raw == nil {
			return nil, nil
		}
		user, err := decodeUser(raw)
		if err != nil {
			return nil, err
		}
		tr.Clear(b.adminUserByIDKey(id))
		tr.Clear(b.adminUserByUsernameKey(user.Username))
		return nil, nil
	})
	if err != nil {
		return err
	}
	b.userCacheMu.Lock()
	defer b.userCacheMu.Unlock()
	b.userCache = make(map[string]*eventstore.User)
	return nil
}

// GetUserByUsername sits on the per-request auth path, so hits are served from
// an in-process cache like the SQLite backend's. Mutations reset the cache.
func (b *Backend) GetUserByUsername(ctx context.Context, username string) (eventstore.User, error) {
	b.userCacheMu.RLock()
	if u, ok := b.userCache[username]; ok && u != nil {
		b.userCacheMu.RUnlock()
		return *u, nil
	}
	b.userCacheMu.RUnlock()

	user, err := b.getUser(b.adminUserByUsernameKey(username), "user not found")
	if err != nil {
		return eventstore.User{}, err
	}
	b.userCacheMu.Lock()
	defer b.userCacheMu.Unlock()
	b.userCache[username] = &user
	return user, nil
}

func (b *Backend) GetUserById(ctx context.Context, id string) (eventstore.User, error) {
	return b.getUser(b.adminUserByIDKey(id), "user not found with id: "+id)
}

func (b *Backend) GetUsersCount(ctx context.Context) (uint32, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(b.usersCountKey()).MustGet()
		if raw == nil {
			return uint32(0), nil
		}
		n, err := strconv.ParseUint(string(raw), 10, 32)
		if err != nil {
			return uint32(0), err
		}
		return uint32(n), nil
	})
	if err != nil {
		return 0, err
	}
	return result.(uint32), nil
}

func (b *Backend) SaveUsersCount(ctx context.Context, count uint32) error {
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(b.usersCountKey(), []byte(strconv.FormatUint(uint64(count), 10)))
		return nil, nil
	})
	return err
}

func (b *Backend) GetEventsCount(ctx context.Context, boundary string) (int, error) {
	if err := b.checkBoundary(boundary); err != nil {
		return 0, err
	}
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(b.eventsCountKey(boundary)).MustGet()
		if raw == nil {
			return -1, nil
		}
		n, err := strconv.Atoi(string(raw))
		if err != nil {
			return -1, nil
		}
		return n, nil
	})
	if err != nil {
		return 0, err
	}
	if n := result.(int); n >= 0 {
		return n, nil
	}
	return b.countEventsPaged(boundary)
}

// countEventsPaged counts the boundary's event keys across bounded chunks. A
// single-transaction count of a large boundary would exceed FoundationDB's 5s
// read window, so the scan is chunked — but every chunk reads the SAME pinned
// read version, so the count is an exact snapshot as of that version even under
// concurrent writes (the dashboard projector seeds its running count from this,
// so a sloppy seed would drift forever). If the boundary is large enough that
// the snapshot ages out of the storage MVCC window mid-count, fall back to an
// unpinned best-effort scan rather than failing the call.
func (b *Backend) countEventsPaged(boundary string) (int, error) {
	readVersion, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		return rt.GetReadVersion().Get()
	})
	if err != nil {
		return 0, err
	}
	total, err := b.countEventsAtVersion(boundary, readVersion.(int64))
	if err == nil {
		return total, nil
	}
	if !isVersionTooOld(err) {
		return 0, err
	}
	b.logger.Warnf("event count for boundary %s outran a consistent snapshot (%v); falling back to approximate count", boundary, err)
	return b.countEventsApprox(boundary)
}

// countEventsAtVersion counts event keys chunk by chunk, every chunk reading the
// same pinned read version so the total is a single consistent snapshot.
func (b *Backend) countEventsAtVersion(boundary string, readVersion int64) (int, error) {
	pr := prefixRange(b.eventPrefix(boundary))
	beginKey := pr.Begin.FDBKey()
	endKey := pr.End.FDBKey()
	total := 0
	for {
		tr, err := b.db.CreateTransaction()
		if err != nil {
			return 0, err
		}
		tr.SetReadVersion(readVersion)
		scanned := 0
		var lastKey fdb.Key
		iter := tr.Snapshot().GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{
			Limit: scanChunkSize,
			Mode:  fdb.StreamingModeWantAll,
		}).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				tr.Cancel()
				return 0, err
			}
			lastKey = kv.Key
			scanned++
		}
		tr.Cancel()
		total += scanned
		if scanned < scanChunkSize {
			return total, nil
		}
		beginKey = keyAfter(lastKey)
	}
}

// countEventsApprox is the fallback for boundaries too large to hold one
// snapshot: independent chunk transactions, so the total is approximate under
// concurrent writes but the call always completes.
func (b *Backend) countEventsApprox(boundary string) (int, error) {
	pr := prefixRange(b.eventPrefix(boundary))
	beginKey := pr.Begin.FDBKey()
	endKey := pr.End.FDBKey()
	total := 0
	for {
		var scanned int
		var lastKey fdb.Key
		_, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
			iter := rt.GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{
				Limit: scanChunkSize,
				Mode:  fdb.StreamingModeWantAll,
			}).Iterator()
			scanned = 0
			for iter.Advance() {
				kv, err := iter.Get()
				if err != nil {
					return nil, err
				}
				lastKey = kv.Key
				scanned++
			}
			return nil, nil
		})
		if err != nil {
			return 0, err
		}
		total += scanned
		if scanned < scanChunkSize {
			return total, nil
		}
		beginKey = keyAfter(lastKey)
	}
}

// isVersionTooOld reports whether err is FoundationDB's transaction_too_old
// (1007) — the pinned read version fell out of the storage MVCC window.
func isVersionTooOld(err error) bool {
	var fe fdb.Error
	if errors.As(err, &fe) {
		return fe.Code == 1007
	}
	return false
}

func isTransactionTooLarge(err error) bool {
	var fe fdb.Error
	if errors.As(err, &fe) {
		return fe.Code == 2101
	}
	return false
}

func (b *Backend) SaveEventCount(ctx context.Context, count int, boundary string) error {
	if err := b.checkBoundary(boundary); err != nil {
		return err
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(b.eventsCountKey(boundary), []byte(strconv.Itoa(count)))
		return nil, nil
	})
	return err
}

func (b *Backend) CreateBoundaryIndex(
	ctx context.Context,
	boundary, name string,
	fields []eventstore.BoundaryIndexField,
	conditions []eventstore.BoundaryIndexCondition,
	combinator string,
) error {
	if err := contextStatusErr(ctx); err != nil {
		return err
	}
	if err := b.checkBoundary(boundary); err != nil {
		return err
	}
	if err := validateIdentifier(name); err != nil {
		return err
	}
	if len(fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}
	if combinator == "" {
		combinator = eventstore.IndexCombinatorAND
	}
	if combinator != eventstore.IndexCombinatorAND && combinator != eventstore.IndexCombinatorOR {
		return fmt.Errorf("invalid combinator %q", combinator)
	}
	def := indexDefinition{
		Name:       name,
		Generation: uuid.NewString(),
		Fields:     fields,
		Conditions: conditions,
		Combinator: combinator,
		State:      indexStateBuilding,
	}
	value, err := json.Marshal(def)
	if err != nil {
		return err
	}
	// Write index metadata first so concurrent and subsequent Saves self-index new
	// events. Existing events are backfilled below in bounded chunks to stay within
	// FoundationDB's 5s / 10MB transaction limits. Backfill is idempotent: if it
	// fails partway, re-running CreateBoundaryIndex re-scans from the start and
	// completes it.
	if _, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		tr.Set(b.indexMetaKey(boundary, name), value)
		tr.Set(b.indexEpochKey(boundary), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		tr.ClearRange(prefixRange(b.indexPrefix(boundary, name)))
		return nil, nil
	}); err != nil {
		return err
	}

	pr := prefixRange(b.eventPrefix(boundary))
	beginKey := pr.Begin.FDBKey()
	endKey := pr.End.FDBKey()
	for {
		if err := contextStatusErr(ctx); err != nil {
			return err
		}
		var lastKey fdb.Key
		var scanned int
		if _, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			if err := contextStatusErr(ctx); err != nil {
				return nil, err
			}
			iter := tr.GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{
				Limit: backfillChunkSize,
				Mode:  fdb.StreamingModeWantAll,
			}).Iterator()
			scanned = 0
			for iter.Advance() {
				if err := contextStatusErr(ctx); err != nil {
					return nil, err
				}
				kv, err := iter.Get()
				if err != nil {
					return nil, err
				}
				lastKey = kv.Key
				scanned++
				tx, gid, err := eventPositionFromKey(kv.Key)
				if err != nil {
					return nil, err
				}
				_, data, err := decodeEventRecord(kv.Value)
				if err != nil {
					return nil, err
				}
				if eventMatchesIndexConditions(data, def) {
					if key, ok := b.indexKeyAtPosition(boundary, def, data, &eventstore.Position{CommitPosition: tx, PreparePosition: gid}); ok {
						tr.Set(key, []byte{})
					}
				}
			}
			return nil, nil
		}); err != nil {
			return err
		}
		if scanned < backfillChunkSize {
			break
		}
		beginKey = keyAfter(lastKey)
	}
	def.State = indexStateReady
	readyValue, err := json.Marshal(def)
	if err != nil {
		return err
	}
	var finalErr error
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		finalErr = nil
		raw := tr.Get(b.indexMetaKey(boundary, name)).MustGet()
		if raw == nil {
			tr.ClearRange(prefixRange(b.indexEntryPrefix(boundary, def)))
			finalErr = fmt.Errorf("index %s was dropped before backfill completed", name)
			return nil, nil
		}
		var current indexDefinition
		if err := json.Unmarshal(raw, &current); err != nil {
			return nil, err
		}
		if current.Generation != def.Generation || current.State != indexStateBuilding {
			tr.ClearRange(prefixRange(b.indexEntryPrefix(boundary, def)))
			finalErr = fmt.Errorf("index %s changed before backfill completed", name)
			return nil, nil
		}
		tr.Set(b.indexMetaKey(boundary, name), readyValue)
		tr.Set(b.indexEpochKey(boundary), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		return nil, nil
	})
	if err != nil {
		return err
	}
	if finalErr != nil {
		return finalErr
	}
	return nil
}

func (b *Backend) DropBoundaryIndex(ctx context.Context, boundary, name string) error {
	if err := contextStatusErr(ctx); err != nil {
		return err
	}
	if err := b.checkBoundary(boundary); err != nil {
		return err
	}
	if err := validateIdentifier(name); err != nil {
		return err
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(b.indexMetaKey(boundary, name))
		tr.Set(b.indexEpochKey(boundary), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		tr.ClearRange(prefixRange(b.indexPrefix(boundary, name)))
		return nil, nil
	})
	return err
}

func (b *Backend) getUser(key fdb.Key, notFound string) (eventstore.User, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(key).MustGet()
		if raw == nil {
			return eventstore.User{}, errors.New(notFound)
		}
		return decodeUser(raw)
	})
	if err != nil {
		return eventstore.User{}, err
	}
	return result.(eventstore.User), nil
}

func (b *Backend) query(ctx context.Context, req *eventstore.GetEventsRequest) (eventstore.ReadEventBatch, error) {
	result, err := b.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		indexes, err := b.loadIndexes(rt, req.Boundary)
		if err != nil {
			return nil, err
		}
		indexes = readyIndexes(indexes)

		eventsByPosition := make(map[[2]int64]eventstore.ReadEvent)
		for _, criterion := range criteriaAsMaps(req.Query) {
			if err := contextStatusErr(ctx); err != nil {
				return nil, err
			}
			idx, ok := chooseCoveringIndex(indexes, criterion)
			if !ok {
				return nil, b.unindexedQueryErr(req.Boundary, criterion)
			}
			candidates, err := b.scanIndexCandidates(ctx, rt, req.Boundary, idx, criterion, req.FromPosition, req.Direction, int(req.Count))
			if err != nil {
				return nil, err
			}
			// scanIndexCandidates already restricted to this criterion's covering
			// index slice (exact field/condition match) and applied the cursor, so
			// candidates need only be de-duplicated across criteria here — no
			// per-event re-parse of the payload.
			for i := range candidates {
				event := candidates[i]
				eventsByPosition[[2]int64{event.CommitPosition, event.PreparePosition}] = event
			}
		}
		events := make(eventstore.ReadEventBatch, 0, len(eventsByPosition))
		for _, event := range eventsByPosition {
			events = append(events, event)
		}
		sort.Slice(events, func(i, j int) bool {
			left, right := events[i], events[j]
			less := left.CommitPosition < right.CommitPosition ||
				(left.CommitPosition == right.CommitPosition && left.PreparePosition < right.PreparePosition)
			if req.Direction == eventstore.Direction_DESC {
				return !less && (left.CommitPosition != right.CommitPosition || left.PreparePosition != right.PreparePosition)
			}
			return less
		})
		count := int(req.Count)
		if count > 0 && len(events) > count {
			events = events[:count]
		}
		return events, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(eventstore.ReadEventBatch), nil
}

func (b *Backend) scanIndexCandidates(ctx context.Context, rt fdb.ReadTransaction, boundary string, idx indexDefinition, criterion map[string]string, from *eventstore.Position, direction eventstore.Direction, count int) (eventstore.ReadEventBatch, error) {
	pr := prefixRange(b.indexLookupPrefix(boundary, idx, criterion))
	beginKey := pr.Begin.FDBKey()
	endKey := pr.End.FDBKey()
	reverse := direction == eventstore.Direction_DESC
	if from != nil {
		if from.CommitPosition < 0 || from.PreparePosition < 0 {
			if reverse {
				return nil, nil
			}
		} else {
			cursor := b.indexCursorKey(boundary, idx, criterion, from)
			if reverse {
				endKey = keyAfter(cursor)
			} else {
				beginKey = cursor
			}
		}
	}
	if count <= 0 {
		count = int(eventstore.DefaultReadBatchSize)
	}
	iter := rt.GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{
		Limit:   count,
		Mode:    fdb.StreamingModeWantAll,
		Reverse: reverse,
	}).Iterator()
	events := make(eventstore.ReadEventBatch, 0, count)
	for iter.Advance() {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		kv, err := iter.Get()
		if err != nil {
			return nil, err
		}
		tx, gid, err := indexPositionFromKey(kv.Key)
		if err != nil {
			return nil, err
		}
		pos := &eventstore.Position{CommitPosition: tx, PreparePosition: gid}
		if !positionMatches(pos, from, direction) {
			continue
		}
		event, ok, err := b.getReadEventByPosition(rt, boundary, tx, gid)
		if err != nil {
			return nil, err
		}
		// The index slice is keyed by this criterion's exact field values and
		// equality conditions (checked when the entry was written), so every
		// entry here matches the full criterion — no payload re-parse needed.
		if ok {
			events = append(events, event)
		}
	}
	return events, nil
}

func (b *Backend) scanEvents(ctx context.Context, rt fdb.ReadTransaction, req *eventstore.GetEventsRequest, limit int) (eventstore.ReadEventBatch, error) {
	if limit <= 0 {
		limit = int(req.Count)
	}
	if limit <= 0 {
		limit = int(eventstore.DefaultReadBatchSize)
	}
	begin, end := b.eventRangeForCursor(req.Boundary, req.FromPosition, req.Direction)
	iter := rt.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{
		Limit:   limit,
		Mode:    fdb.StreamingModeWantAll,
		Reverse: req.Direction == eventstore.Direction_DESC,
	}).Iterator()
	events := make(eventstore.ReadEventBatch, 0, limit)
	for iter.Advance() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		kv, err := iter.Get()
		if err != nil {
			return nil, err
		}
		tx, gid, err := eventPositionFromKey(kv.Key)
		if err != nil {
			return nil, err
		}
		event, err := readEventFromRecord(kv.Value, tx, gid)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		if len(events) >= limit {
			break
		}
	}
	return events, nil
}

// eventRangeForCursor seeks the event range to the read cursor instead of
// filtering after the fact — otherwise the range Limit is consumed by
// pre-cursor keys and paging returns nothing once the cursor passes the first
// Limit keys of the boundary. The cursor is inclusive in both directions,
// matching the `>=`/`<=` semantics of the SQL backends. Positions with a
// negative component (the not-exists sentinel) mean "no cursor".
func (b *Backend) eventRangeForCursor(boundary string, from *eventstore.Position, direction eventstore.Direction) (fdb.KeyConvertible, fdb.KeyConvertible) {
	pr := prefixRange(b.eventPrefix(boundary))
	begin, end := pr.Begin, pr.End
	if from == nil {
		return begin, end
	}
	if from.CommitPosition < 0 || from.PreparePosition < 0 {
		if direction == eventstore.Direction_DESC {
			// SQL parity: `(tx, gid) <= (-1, -1)` matches nothing.
			return begin, begin
		}
		// ASC from the not-exists sentinel reads from the beginning — the
		// publisher's first catch-up after an empty checkpoint depends on this.
		return begin, end
	}
	cursor := b.eventKeyForPosition(boundary, from)
	if direction == eventstore.Direction_DESC {
		// keyAfter keeps the cursor key itself inside the half-open range.
		return begin, keyAfter(cursor)
	}
	return cursor, end
}

// latestMatchingPosition resolves the newest event position matching the
// consistency criteria using only ready covering indexes. A covering index
// slice fully encodes the match — field values live in the key, and condition
// membership was checked when the entry was written — so the newest entry's
// versionstamp IS the answer. No event records are fetched inside the write
// transaction: each criterion costs one Limit-1 reverse range read regardless
// of how long the aggregate's history is.
func (b *Backend) latestMatchingPosition(tr fdb.Transaction, boundary string, query *eventstore.Query) (int64, int64, error) {
	indexes, err := b.loadIndexes(tr, boundary)
	if err != nil {
		return -1, -1, err
	}
	indexes = readyIndexes(indexes)
	bestTx, bestGid := int64(-1), int64(-1)
	found := false
	for _, criterion := range criteriaAsMaps(query) {
		idx, ok := chooseCoveringIndex(indexes, criterion)
		if !ok {
			// Fail closed: an unindexed consistency condition cannot be checked
			// correctly inside the write transaction without scanning the whole
			// boundary (blowing FDB's txn limits and creating a huge conflict
			// range). A silent bounded scan could miss a conflicting event and
			// wrongly pass the optimistic lock.
			return -1, -1, b.unindexedConsistencyErr(boundary, criterion)
		}
		// Scope the conflict range to this criterion's index slice (one
		// aggregate). Only a concurrent write to the SAME aggregate forces a
		// retry; commands on other aggregates in this boundary commit in
		// parallel. This is what makes within-boundary writes scale.
		slice := prefixRange(b.indexLookupPrefix(boundary, idx, criterion))
		if err := tr.AddReadConflictRange(slice); err != nil {
			return -1, -1, err
		}
		iter := tr.GetRange(slice, fdb.RangeOptions{
			Limit:   1,
			Mode:    fdb.StreamingModeWantAll,
			Reverse: true,
		}).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return -1, -1, err
			}
			tx, gid, err := indexPositionFromKey(kv.Key)
			if err != nil {
				return -1, -1, err
			}
			if !found || tx > bestTx || (tx == bestTx && gid > bestGid) {
				bestTx, bestGid = tx, gid
				found = true
			}
		}
	}
	return bestTx, bestGid, nil
}

func (b *Backend) unindexedConsistencyErr(boundary string, criterion map[string]string) error {
	return b.unindexedCriteriaErr("consistency condition", boundary, criterion,
		"create one with CreateBoundaryIndex before using it in a consistency condition")
}

func (b *Backend) unindexedQueryErr(boundary string, criterion map[string]string) error {
	return b.unindexedCriteriaErr("query", boundary, criterion,
		"create one with CreateBoundaryIndex before querying these criteria")
}

func (b *Backend) unindexedCriteriaErr(kind, boundary string, criterion map[string]string, guidance string) error {
	keys := make([]string, 0, len(criterion))
	for key := range criterion {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return statuscode.Errorf(statuscode.FailedPrecondition,
		"%s on boundary %s references keys %v with no ready covering index; %s",
		kind, boundary, keys, guidance)
}

func (b *Backend) getReadEventByPosition(rt fdb.ReadTransaction, boundary string, tx, gid int64) (eventstore.ReadEvent, bool, error) {
	key := b.eventKeyForPosition(boundary, &eventstore.Position{CommitPosition: tx, PreparePosition: gid})
	raw := rt.Get(key).MustGet()
	if raw == nil {
		return eventstore.ReadEvent{}, false, nil
	}
	event, err := readEventFromRecord(raw, tx, gid)
	return event, err == nil, err
}

func (b *Backend) loadIndexes(rt fdb.ReadTransaction, boundary string) ([]indexDefinition, error) {
	iter := rt.GetRange(prefixRange(b.indexMetaPrefix(boundary)), fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
	var indexes []indexDefinition
	for iter.Advance() {
		kv, err := iter.Get()
		if err != nil {
			return nil, err
		}
		var def indexDefinition
		if err := json.Unmarshal(kv.Value, &def); err != nil {
			return nil, err
		}
		if def.State == "" {
			def.State = indexStateReady
		}
		indexes = append(indexes, def)
	}
	return indexes, nil
}

func (b *Backend) checkBoundary(boundary string) error {
	b.boundaryMu.RLock()
	defer b.boundaryMu.RUnlock()
	if _, ok := b.boundaries[boundary]; !ok {
		return statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	return nil
}

func (b *Backend) ProvisionBoundary(ctx context.Context, definition boundarymodel.Definition) error {
	if b == nil {
		return fmt.Errorf("FoundationDB boundary provisioner is not configured")
	}
	if err := b.validateBoundaryDefinition(definition); err != nil {
		return err
	}
	return b.ensureBoundaryMarker(ctx, definition.Name)
}

func (b *Backend) InstallBoundary(_ context.Context, definition boundarymodel.Definition) error {
	if b == nil {
		return fmt.Errorf("FoundationDB boundary installer is not configured")
	}
	if err := b.validateBoundaryDefinition(definition); err != nil {
		return err
	}
	b.boundaryMu.Lock()
	b.boundaries[definition.Name] = struct{}{}
	b.boundaryMu.Unlock()
	return nil
}

func (b *Backend) validateBoundaryDefinition(definition boundarymodel.Definition) error {
	if !strings.EqualFold(strings.TrimSpace(definition.Placement.Backend), "foundationdb") {
		return fmt.Errorf("boundary %s uses unsupported backend %q", definition.Name, definition.Placement.Backend)
	}
	if err := boundarymodel.ValidateName(definition.Name); err != nil {
		return fmt.Errorf("invalid boundary name %s: %w", definition.Name, err)
	}
	if strings.TrimSpace(definition.Placement.Namespace) != b.root {
		return fmt.Errorf("FoundationDB boundary %s must use namespace %q", definition.Name, b.root)
	}
	return nil
}

func (b *Backend) ensureBoundaryMarker(ctx context.Context, boundary string) error {
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return err
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		tr.Set(b.boundaryMarkerKey(boundary), []byte{1})
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("persist FoundationDB boundary %s: %w", boundary, err)
	}
	return nil
}

func (b *Backend) tupleKey(parts ...tuple.TupleElement) fdb.Key {
	all := make(tuple.Tuple, 0, len(parts)+1)
	all = append(all, b.root)
	all = append(all, parts...)
	return fdb.Key(all.Pack())
}

func (b *Backend) eventPrefix(boundary string) fdb.Key {
	return b.tupleKey(boundary, "event")
}

func (b *Backend) boundaryMarkerKey(boundary string) fdb.Key {
	return b.tupleKey(boundary, "boundary")
}

// eventVersionstampKey builds an event key whose position is filled in by FDB at
// commit. userVersion orders events within the batch. Use with SetVersionstampedKey.
func (b *Backend) eventVersionstampKey(boundary string, userVersion uint16) (fdb.Key, error) {
	t := tuple.Tuple{b.root, boundary, "event", tuple.IncompleteVersionstamp(userVersion)}
	packed, err := t.PackWithVersionstamp(nil)
	if err != nil {
		return nil, err
	}
	return fdb.Key(packed), nil
}

// eventKeyForPosition reconstructs the stored key for a known position.
func (b *Backend) eventKeyForPosition(boundary string, pos *eventstore.Position) fdb.Key {
	t := tuple.Tuple{b.root, boundary, "event", versionstampFromPosition(pos)}
	return fdb.Key(t.Pack())
}

func (b *Backend) signalKey(boundary string) fdb.Key {
	return b.tupleKey(boundary, "signal")
}

func (b *Backend) lastPublishedKey(boundary string) fdb.Key {
	return b.tupleKey(boundary, "last_published")
}

func (b *Backend) indexMetaPrefix(boundary string) fdb.Key {
	return b.tupleKey(boundary, "index_meta")
}

func (b *Backend) indexMetaKey(boundary, name string) fdb.Key {
	return b.tupleKey(boundary, "index_meta", name)
}

func (b *Backend) indexEpochKey(boundary string) fdb.Key {
	return b.tupleKey(boundary, "index_epoch")
}

func (b *Backend) indexPrefix(boundary, name string) fdb.Key {
	return b.tupleKey(boundary, "index", name)
}

func (b *Backend) indexEntryPrefix(boundary string, idx indexDefinition) fdb.Key {
	return fdb.Key(b.indexKeyParts(boundary, idx).Pack())
}

func (b *Backend) indexKeyParts(boundary string, idx indexDefinition) tuple.Tuple {
	parts := tuple.Tuple{b.root, boundary, "index", idx.Name}
	if idx.Generation != "" {
		parts = append(parts, idx.Generation)
	}
	return parts
}

func (b *Backend) indexLookupPrefix(boundary string, idx indexDefinition, criterion map[string]string) fdb.Key {
	parts := b.indexKeyParts(boundary, idx)
	for _, field := range idx.Fields {
		parts = append(parts, criterion[field.JsonKey])
	}
	return fdb.Key(parts.Pack())
}

// indexVersionstampKey builds a live index entry whose position is filled in by
// FDB at commit, sharing userVersion with its event. Use with SetVersionstampedKey.
func (b *Backend) indexVersionstampKey(boundary string, idx indexDefinition, data map[string]any, userVersion uint16) (fdb.Key, bool, error) {
	parts := b.indexKeyParts(boundary, idx)
	for _, field := range idx.Fields {
		value, ok := data[field.JsonKey]
		if !ok {
			return nil, false, nil
		}
		parts = append(parts, indexValueString(value))
	}
	parts = append(parts, tuple.IncompleteVersionstamp(userVersion))
	packed, err := parts.PackWithVersionstamp(nil)
	if err != nil {
		return nil, false, err
	}
	return fdb.Key(packed), true, nil
}

// indexCursorKey returns the index-entry key at a known position inside one
// criterion's lookup slice. Used to seek paged index scans to a read cursor.
func (b *Backend) indexCursorKey(boundary string, idx indexDefinition, criterion map[string]string, pos *eventstore.Position) fdb.Key {
	parts := b.indexKeyParts(boundary, idx)
	for _, field := range idx.Fields {
		parts = append(parts, criterion[field.JsonKey])
	}
	parts = append(parts, versionstampFromPosition(pos))
	return fdb.Key(parts.Pack())
}

// indexKeyAtPosition builds an index entry for an already-committed event, used
// when backfilling an index. The position is known, so the versionstamp is complete.
func (b *Backend) indexKeyAtPosition(boundary string, idx indexDefinition, data map[string]any, pos *eventstore.Position) (fdb.Key, bool) {
	parts := b.indexKeyParts(boundary, idx)
	for _, field := range idx.Fields {
		value, ok := data[field.JsonKey]
		if !ok {
			return nil, false
		}
		parts = append(parts, indexValueString(value))
	}
	parts = append(parts, versionstampFromPosition(pos))
	return fdb.Key(parts.Pack()), true
}

// versionstampFromPosition reconstructs the 12-byte tuple versionstamp encoded by
// a Position. CommitPosition holds the 8-byte commit version; PreparePosition packs
// the 2-byte transaction batch order (high 16 bits) and 2-byte user version (low).
func versionstampFromPosition(pos *eventstore.Position) tuple.Versionstamp {
	var tv [10]byte
	binary.BigEndian.PutUint64(tv[0:8], uint64(pos.CommitPosition))
	binary.BigEndian.PutUint16(tv[8:10], uint16(pos.PreparePosition>>16))
	return tuple.Versionstamp{
		TransactionVersion: tv,
		UserVersion:        uint16(pos.PreparePosition & 0xffff),
	}
}

func positionFromVersionstamp(vs tuple.Versionstamp) *eventstore.Position {
	version := binary.BigEndian.Uint64(vs.TransactionVersion[0:8])
	batch := binary.BigEndian.Uint16(vs.TransactionVersion[8:10])
	return &eventstore.Position{
		CommitPosition:  int64(version),
		PreparePosition: int64(batch)<<16 | int64(vs.UserVersion),
	}
}

func (b *Backend) adminUserByIDPrefix() fdb.Key {
	return b.tupleKey("admin", "user_by_id")
}

func (b *Backend) adminUserByIDKey(id string) fdb.Key {
	return b.tupleKey("admin", "user_by_id", id)
}

func (b *Backend) adminUserByUsernameKey(username string) fdb.Key {
	return b.tupleKey("admin", "user_by_username", username)
}

func (b *Backend) projectorKey(name string) fdb.Key {
	return b.tupleKey("admin", "projector", name)
}

func (b *Backend) usersCountKey() fdb.Key {
	return b.tupleKey("admin", "users_count")
}

func (b *Backend) eventsCountKey(boundary string) fdb.Key {
	return b.tupleKey(boundary, "events_count")
}

func eventPositionFromKey(key fdb.Key) (int64, int64, error) {
	return positionFromKey(key, "event")
}

func indexPositionFromKey(key fdb.Key) (int64, int64, error) {
	return positionFromKey(key, "index")
}

// positionFromKey decodes the trailing versionstamp of an event or index key into
// (commitPosition, preparePosition).
func positionFromKey(key fdb.Key, kind string) (int64, int64, error) {
	unpacked, err := tuple.Unpack(key)
	if err != nil {
		return 0, 0, err
	}
	if len(unpacked) == 0 {
		return 0, 0, fmt.Errorf("invalid %s key", kind)
	}
	vs, ok := unpacked[len(unpacked)-1].(tuple.Versionstamp)
	if !ok {
		return 0, 0, fmt.Errorf("invalid %s key: missing versionstamp", kind)
	}
	pos := positionFromVersionstamp(vs)
	return pos.CommitPosition, pos.PreparePosition, nil
}

func prefixRange(prefix fdb.Key) fdb.KeyRange {
	r, err := fdb.PrefixRange(prefix)
	if err != nil {
		panic(err)
	}
	return r
}

// keyAfter returns the smallest key strictly greater than key, used as an
// exclusive cursor when paginating a range scan across transactions.
func keyAfter(key fdb.Key) fdb.Key {
	next := make(fdb.Key, len(key)+1)
	copy(next, key)
	next[len(key)] = 0x00
	return next
}

type fdbSignal struct {
	db       fdb.Database
	key      fdb.Key
	fallback time.Duration
	stopped  chan struct{}
	once     sync.Once
}

func (s *fdbSignal) Wait(ctx context.Context) error {
	tr, err := s.db.CreateTransaction()
	if err != nil {
		return s.poll(ctx)
	}
	watch := tr.Watch(s.key)
	if err := tr.Commit().Get(); err != nil {
		watch.Cancel()
		return s.poll(ctx)
	}

	done := make(chan error, 1)
	go func() {
		done <- watch.Get()
	}()

	select {
	case <-ctx.Done():
		watch.Cancel()
		return ctx.Err()
	case <-s.stopped:
		watch.Cancel()
		return context.Canceled
	case err := <-done:
		if err != nil {
			return s.poll(ctx)
		}
		return nil
	}
}

func (s *fdbSignal) Stop() {
	s.once.Do(func() {
		close(s.stopped)
	})
}

func (s *fdbSignal) poll(ctx context.Context) error {
	timer := time.NewTimer(s.fallback)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped:
		return context.Canceled
	case <-timer.C:
		return nil
	}
}
