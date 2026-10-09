package postgres

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	orisun "github.com/OrisunLabs/Orisun/orisun"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PGNotifyListener struct {
	conn                  *pgx.Conn
	boundarySchemaMapping map[string]config.BoundaryToPostgresSchemaMapping
	logger                logging.Logger

	mu             sync.Mutex
	signals        map[string]chan struct{}
	channelSignals map[string]chan struct{}
	done           chan struct{}

	controlMu  sync.Mutex
	pending    []*boundaryListenRequest
	waitCancel context.CancelFunc
}

type boundaryListenRequest struct {
	boundary string
	result   chan error
}

const pgNotifyChannelPrefix = "orisun_events_"

func pgNotifyChannelForBoundary(boundary string) string {
	return fmt.Sprintf("%s%x", pgNotifyChannelPrefix, md5.Sum([]byte(boundary)))
}

func NewPGNotifyListener(
	ctx context.Context,
	connStr string,
	boundarySchemaMapping map[string]config.BoundaryToPostgresSchemaMapping,
	logger logging.Logger,
) (*PGNotifyListener, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("pg_notify listener connect: %w", err)
	}

	registeredBoundaries := make(map[string]config.BoundaryToPostgresSchemaMapping, len(boundarySchemaMapping))
	signals := make(map[string]chan struct{}, len(boundarySchemaMapping))
	channelSignals := make(map[string]chan struct{}, len(boundarySchemaMapping))
	for boundary, mapping := range boundarySchemaMapping {
		channel := pgNotifyChannelForBoundary(boundary)
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
			conn.Close(ctx)
			return nil, fmt.Errorf("LISTEN %s: %w", channel, err)
		}
		registeredBoundaries[boundary] = mapping
		signal := make(chan struct{}, 1)
		signals[boundary] = signal
		channelSignals[channel] = signal
	}

	return &PGNotifyListener{
		conn:                  conn,
		boundarySchemaMapping: registeredBoundaries,
		logger:                logger,
		signals:               signals,
		channelSignals:        channelSignals,
		done:                  make(chan struct{}),
	}, nil
}

// EnsureBoundary installs LISTEN routing for a boundary on the listener-owned
// connection. It wakes WaitForNotification instead of using pgx.Conn
// concurrently.
func (l *PGNotifyListener) EnsureBoundary(ctx context.Context, boundary string) error {
	if l == nil {
		return fmt.Errorf("pg_notify listener is not configured")
	}
	if err := validateBoundaryName(boundary); err != nil {
		return fmt.Errorf("invalid boundary name %s: %w", boundary, err)
	}

	l.mu.Lock()
	_, exists := l.signals[boundary]
	l.mu.Unlock()
	if exists {
		return nil
	}

	request := &boundaryListenRequest{
		boundary: boundary,
		result:   make(chan error, 1),
	}
	l.controlMu.Lock()
	l.pending = append(l.pending, request)
	if l.waitCancel != nil {
		l.waitCancel()
	}
	l.controlMu.Unlock()

	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return fmt.Errorf("pg_notify listener stopped before registering boundary %s", boundary)
	}
}

// Start owns l.conn exclusively for the lifetime of the goroutine: it is the
// only place the connection is read, swapped (via reconnect), or closed. No
// other goroutine may touch l.conn — pgx.Conn is not safe for concurrent use.
func (l *PGNotifyListener) Start(ctx context.Context) {
	defer close(l.done)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		l.conn.Close(closeCtx)
	}()

	for {
		if request := l.nextListenRequest(); request != nil {
			request.result <- l.listenBoundary(ctx, request.boundary)
			continue
		}

		waitCtx, cancelWait := context.WithCancel(ctx)
		if !l.armNotificationWait(cancelWait) {
			cancelWait()
			continue
		}
		notification, err := l.conn.WaitForNotification(waitCtx)
		l.disarmNotificationWait()
		cancelWait()
		// pgx can return a buffered notification together with a context error
		// when a registration request wakes the wait. Route it before handling
		// the error so dynamic LISTEN work cannot make a real wake-up disappear.
		if notification != nil {
			l.dispatch(notification)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.Canceled) {
				continue
			}
			l.logger.Errorf("PG LISTEN connection error: %v — reconnecting", err)
			if reconnectErr := l.reconnect(ctx); reconnectErr != nil {
				if ctx.Err() != nil {
					return
				}
				l.logger.Errorf("PG LISTEN reconnect failed: %v", reconnectErr)
				return
			}
			continue
		}
	}
}

func (l *PGNotifyListener) nextListenRequest() *boundaryListenRequest {
	l.controlMu.Lock()
	defer l.controlMu.Unlock()
	if len(l.pending) == 0 {
		return nil
	}
	request := l.pending[0]
	l.pending[0] = nil
	l.pending = l.pending[1:]
	return request
}

func (l *PGNotifyListener) armNotificationWait(cancel context.CancelFunc) bool {
	l.controlMu.Lock()
	defer l.controlMu.Unlock()
	if len(l.pending) > 0 {
		return false
	}
	l.waitCancel = cancel
	return true
}

func (l *PGNotifyListener) disarmNotificationWait() {
	l.controlMu.Lock()
	l.waitCancel = nil
	l.controlMu.Unlock()
}

func (l *PGNotifyListener) listenBoundary(ctx context.Context, boundary string) error {
	l.mu.Lock()
	_, exists := l.signals[boundary]
	l.mu.Unlock()
	if exists {
		return nil
	}

	channel := pgNotifyChannelForBoundary(boundary)
	if _, err := l.conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return fmt.Errorf("LISTEN %s: %w", channel, err)
	}

	signal := make(chan struct{}, 1)
	l.mu.Lock()
	// Duplicate requests can be queued while the first LISTEN is in flight.
	// Preserve the first channel so existing EventSignal values remain valid.
	if _, exists := l.signals[boundary]; !exists {
		if l.boundarySchemaMapping == nil {
			l.boundarySchemaMapping = make(map[string]config.BoundaryToPostgresSchemaMapping)
		}
		l.signals[boundary] = signal
		l.channelSignals[channel] = signal
		l.boundarySchemaMapping[boundary] = config.BoundaryToPostgresSchemaMapping{Boundary: boundary}
	}
	l.mu.Unlock()
	l.logger.Infof("PG LISTEN registered dynamic boundary %s on channel %s", boundary, channel)
	return nil
}

func (l *PGNotifyListener) dispatch(notification *pgconn.Notification) {
	l.mu.Lock()
	ch, ok := l.channelSignals[notification.Channel]
	l.mu.Unlock()

	if !ok {
		return
	}

	// Non-blocking send — coalesce if a signal is already pending.
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (l *PGNotifyListener) reconnect(ctx context.Context) error {
	backoff := orisun.Backoff{Base: 500 * time.Millisecond, Max: 10 * time.Second}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := backoff.Wait(ctx); err != nil {
			return err
		}

		conn, err := pgx.Connect(ctx, l.conn.Config().ConnString())
		if err != nil {
			l.logger.Errorf("PG LISTEN reconnect attempt failed: %v", err)
			continue
		}

		// Re-issue startup and dynamically registered LISTEN commands on the
		// new connection.
		allOk := true
		for _, boundary := range l.listenedBoundaries() {
			channel := pgNotifyChannelForBoundary(boundary)
			if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
				l.logger.Errorf("Re-LISTEN %s failed: %v", channel, err)
				allOk = false
				break
			}
		}
		if !allOk {
			conn.Close(ctx)
			continue
		}

		// Swap the old connection for the new one. Safe without locking: only
		// the Start goroutine touches l.conn.
		oldConn := l.conn
		l.conn = conn
		oldConn.Close(ctx)

		l.logger.Info("PG LISTEN reconnected and re-listened on all channels")
		l.wakeRegisteredBoundaries()
		return nil
	}
}

// Notifications received during a disconnected LISTEN session cannot be
// replayed. Wake every registered relay after restoring the session; each
// subscriber's independent reconciliation remains the recovery guarantee.
func (l *PGNotifyListener) wakeRegisteredBoundaries() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, signal := range l.signals {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
}

func (l *PGNotifyListener) listenedBoundaries() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	boundaries := make([]string, 0, len(l.boundarySchemaMapping))
	for boundary := range l.boundarySchemaMapping {
		boundaries = append(boundaries, boundary)
	}
	return boundaries
}

func (l *PGNotifyListener) Signal(boundary string) orisun.EventSignal {
	l.mu.Lock()
	ch, ok := l.signals[boundary]
	l.mu.Unlock()

	if !ok {
		// Only registered boundaries have a notification signal.
		return nil
	}

	return &pgNotifySignal{
		notifyCh: ch,
	}
}

// Close waits for the Start goroutine to stop (it closes the connection on
// exit). The caller must cancel the context passed to Start to trigger
// shutdown; Close itself returns once Start has finished or ctx is done.
func (l *PGNotifyListener) Close(ctx context.Context) {
	select {
	case <-l.done:
	case <-ctx.Done():
	}
}

type pgNotifySignal struct {
	notifyCh chan struct{}
}

func (s *pgNotifySignal) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.notifyCh:
		return nil
	}
}

func (s *pgNotifySignal) Stop() {}
