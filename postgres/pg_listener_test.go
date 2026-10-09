package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLogger struct{}

func (testLogger) IsDebugEnabled() bool  { return false }
func (testLogger) Debug(...any)          {}
func (testLogger) Debugf(string, ...any) {}
func (testLogger) Info(...any)           {}
func (testLogger) Infof(string, ...any)  {}
func (testLogger) Warn(...any)           {}
func (testLogger) Warnf(string, ...any)  {}
func (testLogger) Error(...any)          {}
func (testLogger) Errorf(string, ...any) {}
func (testLogger) Fatal(...any)          {}
func (testLogger) Fatalf(string, ...any) {}

func newTestListener(boundaries ...string) *PGNotifyListener {
	mapping := make(map[string]config.BoundaryToPostgresSchemaMapping, len(boundaries))
	signals := make(map[string]chan struct{}, len(boundaries))
	channelSignals := make(map[string]chan struct{}, len(boundaries))
	for _, b := range boundaries {
		mapping[b] = config.BoundaryToPostgresSchemaMapping{}
		signal := make(chan struct{}, 1)
		signals[b] = signal
		channelSignals[pgNotifyChannelForBoundary(b)] = signal
	}
	return &PGNotifyListener{
		boundarySchemaMapping: mapping,
		logger:                testLogger{},
		signals:               signals,
		channelSignals:        channelSignals,
		done:                  make(chan struct{}),
	}
}

func TestPGNotifyChannelForBoundary(t *testing.T) {
	channel := pgNotifyChannelForBoundary("VeryLongBoundaryNameThatStillPassesValidation_ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	assert.LessOrEqual(t, len(channel), 63)
	assert.Equal(t, channel, pgNotifyChannelForBoundary("VeryLongBoundaryNameThatStillPassesValidation_ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
	assert.NotEqual(t, pgNotifyChannelForBoundary("Orders"), pgNotifyChannelForBoundary("orders"))
	assert.Contains(t, channel, "orisun_events_")
}

func TestPGNotifyReconnectWakeCoalescesForEveryRegisteredBoundary(t *testing.T) {
	listener := newTestListener("orders", "sales")
	listener.wakeRegisteredBoundaries()
	listener.wakeRegisteredBoundaries()
	for boundary, signal := range listener.signals {
		select {
		case <-signal:
		default:
			t.Fatalf("reconnect did not wake %s", boundary)
		}
		select {
		case <-signal:
			t.Fatalf("reconnect wakes did not coalesce for %s", boundary)
		default:
		}
	}
}

func TestPGNotifySignal_Wait(t *testing.T) {
	t.Run("wakes on notify channel", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		s := &pgNotifySignal{notifyCh: ch}
		defer s.Stop()
		ch <- struct{}{}
		require.NoError(t, s.Wait(context.Background()))
	})

	t.Run("waits for a notification without periodic wake-ups", func(t *testing.T) {
		s := &pgNotifySignal{notifyCh: make(chan struct{}, 1)}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, s.Wait(ctx), context.DeadlineExceeded)
	})

	t.Run("returns on canceled context", func(t *testing.T) {
		s := &pgNotifySignal{notifyCh: make(chan struct{}, 1)}
		defer s.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.ErrorIs(t, s.Wait(ctx), context.Canceled)
	})
}

func TestPGNotifyListener_Dispatch(t *testing.T) {
	t.Run("routes notification to the right boundary", func(t *testing.T) {
		l := newTestListener("b1", "b2")
		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("b1")})

		select {
		case <-l.signals["b1"]:
		default:
			t.Fatal("expected b1 signal to fire")
		}
		select {
		case <-l.signals["b2"]:
			t.Fatal("b2 must not fire for a b1 notification")
		default:
		}
	})

	t.Run("coalesces repeated notifications", func(t *testing.T) {
		l := newTestListener("b1")
		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("b1")})
		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("b1")})
		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("b1")})

		<-l.signals["b1"] // exactly one token despite three dispatches
		select {
		case <-l.signals["b1"]:
			t.Fatal("notifications must coalesce into a single pending token")
		default:
		}
	})

	t.Run("ignores unknown boundary", func(t *testing.T) {
		l := newTestListener("b1")
		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("unknown")})
		select {
		case <-l.signals["b1"]:
			t.Fatal("unknown boundary must not fire b1")
		default:
		}
	})

	t.Run("ignores malformed channel", func(t *testing.T) {
		l := newTestListener("b1")
		// Channel equal to the prefix has no boundary suffix — must be a no-op.
		assert.NotPanics(t, func() {
			l.dispatch(&pgconn.Notification{Channel: "orisun_events_"})
			l.dispatch(&pgconn.Notification{Channel: "other"})
		})
	})
}

func TestPGNotifyListener_Signal(t *testing.T) {
	t.Run("known boundary wakes on dispatch", func(t *testing.T) {
		l := newTestListener("b1")
		sig := l.Signal("b1")
		defer sig.Stop()

		l.dispatch(&pgconn.Notification{Channel: pgNotifyChannelForBoundary("b1")})
		require.NoError(t, sig.Wait(context.Background()))
	})

	t.Run("unknown boundary has no signal", func(t *testing.T) {
		l := newTestListener("b1")
		require.Nil(t, l.Signal("nope"))
	})
}

// --- integration: real Postgres LISTEN/NOTIFY -----------------------------

func TestPGNotifyListener_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based integration test in short mode")
	}

	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		if err := container.container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	connStr := fmt.Sprintf(
		"host=%s port=%s user=test password=test dbname=testdb sslmode=disable",
		container.host, container.port,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"b1": {}, "b2": {},
	}
	listener, err := NewPGNotifyListener(ctx, connStr, mapping, testLogger{})
	require.NoError(t, err)
	var listenerBackendPID int
	require.NoError(t, listener.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&listenerBackendPID))
	// Close waits for Start to stop, so the context must be cancelled first.
	defer func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		listener.Close(closeCtx)
	}()

	go listener.Start(ctx)

	sigB1 := listener.Signal("b1")
	defer sigB1.Stop()
	sigB2 := listener.Signal("b2")
	defer sigB2.Stop()

	// Separate connection emits the NOTIFY, mirroring the pg_notify in the
	// insert function.
	notifier, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer notifier.Close(ctx)

	time.Sleep(200 * time.Millisecond) // let LISTEN register

	// Build the channel with Postgres' own md5() exactly as the insert function
	// does. This asserts the Go-side hash (LISTEN) agrees with the SQL-side hash
	// (NOTIFY) — the actual cross-language contract.
	_, err = notifier.Exec(ctx, "SELECT pg_notify('orisun_events_' || md5($1), '7')", "b1")
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	defer waitCancel()
	require.NoError(t, sigB1.Wait(waitCtx), "b1 must wake on its NOTIFY")

	// b2 received no NOTIFY and has catch-up disabled — it must not wake.
	b2Ctx, b2Cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer b2Cancel()
	assert.ErrorIs(t, sigB2.Wait(b2Ctx), context.DeadlineExceeded, "b2 must stay asleep without a NOTIFY")

	// Register a boundary after Start is already blocked in
	// WaitForNotification. EnsureBoundary must wake that wait, execute LISTEN on
	// the listener-owned connection, coalesce concurrent registration, and
	// install routing before returning.
	registrationErrors := make(chan error, 8)
	for range 8 {
		go func() {
			registrationErrors <- listener.EnsureBoundary(ctx, "dynamic")
		}()
	}
	for range 8 {
		require.NoError(t, <-registrationErrors, "dynamic registration must be idempotent")
	}
	sigDynamic := listener.Signal("dynamic")
	defer sigDynamic.Stop()

	_, err = notifier.Exec(ctx, "SELECT pg_notify('orisun_events_' || md5($1), '8')", "dynamic")
	require.NoError(t, err)

	dynamicCtx, dynamicCancel := context.WithTimeout(ctx, 3*time.Second)
	defer dynamicCancel()
	require.NoError(t, sigDynamic.Wait(dynamicCtx), "dynamic boundary must wake on its NOTIFY")

	// Force the listener connection to reconnect. Dynamic registrations must be
	// included in the re-LISTEN set, not just startup mappings.
	var terminated bool
	require.NoError(t, notifier.QueryRow(ctx, "SELECT pg_terminate_backend($1)", listenerBackendPID).Scan(&terminated))
	require.True(t, terminated)
	require.Eventually(t, func() bool {
		if _, notifyErr := notifier.Exec(ctx, "SELECT pg_notify('orisun_events_' || md5($1), '9')", "dynamic"); notifyErr != nil {
			return false
		}
		select {
		case <-listener.signals["dynamic"]:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}, 10*time.Second, 250*time.Millisecond, "dynamic boundary must be re-LISTENed after reconnect")
}
