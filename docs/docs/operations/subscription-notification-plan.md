---
title: Subscription notification redesign plan
description: Replace event payload delivery through NATS with boundary wake-ups and backend-owned subscription reads.
---

# Subscription notification redesign plan

Status: proposed implementation plan, October 8, 2026. This document describes work to do; it does not claim that the notification redesign is implemented.

## 1. Intended outcome and scope

NATS will tell subscriptions that a boundary **may have new events**. Each subscription will fetch matching events from the storage backend, in position order, after its last successfully delivered position. PostgreSQL, SQLite, and FoundationDB will own criteria evaluation for both historical and newly committed events.

This extends the server-side tag-operator work (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`). It removes the need to implement those operators in a separate live-subscription matcher.

Scope includes the server runtime, notification sources, NATS integration, subscription lifecycle, server tests, and documentation. **Do not change Go, Node, or Java clients, their protobuf copies, or their generated bindings.** The subscription RPC request and event response remain unchanged. The canonical tag-operator protobuf change already in progress remains a separate part of the server work.

No application event data or metadata should travel through the new NATS notification stream. This is a complete change to the notification pipeline, not a mode that continues publishing full events alongside hints.

## 2. Current implementation and why it must change

The current flow is:

```text
backend commit
    -> backend signal (PostgreSQL NOTIFY, SQLite notifier, FoundationDB watch)
    -> EventPollingManager reads event batches from the backend
    -> publisher serializes every event and publishes it to JetStream
    -> publisher stores the last published backend position

subscription
    -> initial backend catch-up
    -> choose a JetStream start time using event timestamps / a 10-second overlap
    -> deserialize JetStream events
    -> evaluate query criteria again in Go
    -> deliver matching events
```

Relevant implementation points:

| Area | Current location | Responsibility to change |
| --- | --- | --- |
| Subscription orchestration | `orisun/eventstore.go`, `SubscribeToAllEvents` | Replace separate catch-up/live delivery with one backend drain loop. |
| Live event decoding and filtering | `publishedEventEnvelope`, `neutralPublishedEvent`, `eventMatchesQueryCriteria` | Remove from subscription delivery. |
| Publisher | `EventPollingManager`, `publishEventsLoopWithLease` | Replace event retrieval, serialization, and checkpoint writes with signal forwarding. |
| Backend contracts | `orisun/eventstore_core.go` | Keep notification signaling separate from event retrieval; remove the relay's publisher-checkpoint dependency. |
| Runtime wiring and dynamic boundaries | `server/server.go` and backend runtime constructors | Install notification relays for active boundaries and preserve cleanup. |
| PostgreSQL signal | `postgres/pg_listener.go` | Preserve LISTEN connection ownership, reconnect, dynamic registration, and coalescing. |
| SQLite signal | `sqlite/event_signal.go` | Preserve post-commit notification and cancellation-safe coalescing. |
| FoundationDB signal | `foundationdb/backend.go`, its `fdbSignal` implementation | Preserve commit-linked watch signaling and watch re-registration. |
| Existing tests | `orisun/eventstore_publish_test.go`, `orisun/eventstore_subscription_lifecycle_test.go` | Replace payload-publisher assumptions with relay and backend-drain invariants. |

The current design has two query evaluators and a timestamp-based handoff. The new design must remove both. Simply changing the subscription callback to query the database while keeping the full-event publisher would leave the unnecessary event reads, serialization, network traffic, and checkpoint writes in place.

## 3. Required invariants

1. **The backend is the sole authority for query matches.** Subscription code may validate query structure, positions, and ordering, but must not inspect event data to decide whether criteria match.
2. **Notifications are hints, not progress.** Receiving or acknowledging a notification never advances an event cursor. Notification payloads and NATS sequence numbers are not backend positions.
3. **A successful handler call advances the cursor.** Failed delivery leaves that event eligible for replay. A successful gRPC send is not an application transaction acknowledgement; consumers still own their durable checkpoints and idempotent effects.
4. **Delivery is ascending by `(commit_position, prepare_position)`.** Drain execution is sequential per subscription, including when many notifications arrive concurrently.
5. **Backend reads must expose a stable prefix.** A subscriber must not advance beyond a lower-position event that could become visible later. Preserve PostgreSQL's ASC visibility barrier; do not substitute a DESC maximum as a streaming watermark.
6. **Lost notifications cannot strand events indefinitely.** A subscription-owned reconciliation timer must trigger backend reads independently of the relay and NATS.
7. **At-least-once remains the application contract.** A process failure or client reconnect can replay events after the application's last durable checkpoint. There is no claim of exactly-once delivery.
8. **Slow consumers have bounded memory.** Keep one pending wake-up per subscription, one bounded event batch, and no unbounded queue of event payloads.
9. **Lifecycle ownership is explicit.** Cancellation, lease loss, handler failure, inactive boundaries, and shutdown stop the relevant work and release consumers, timers, watches, and leases.
10. **Backend isolation remains intact.** PostgreSQL-only and SQLite-only binaries and embedding packages remain dependency-clean; SQLite remains incompatible with NATS clustering.

## 4. Target architecture

```text
PostgreSQL commit -> NOTIFY ----+
SQLite commit -> local signal -+-> boundary notification relay
FoundationDB commit -> watch --+       |
                                      v
                               NATS boundary.changed hint
                                      |
                                      v
                              coalesced pending wake-up
                                      |
initial subscription -----------------+
reconciliation timer -----------------+-> one sequential backend drain
NATS reconnect / consumer recreation -+       |
                                              v
                                  backend evaluates complete query
                                              |
                                              v
                                  ordered handler / gRPC delivery
                                              |
                                              v
                                  last successfully delivered position
```

The relay will have no `EventsRetriever` dependency and no publisher-position tracker. This structural constraint prevents the old data-publishing pipeline from surviving inside a renamed component.

### 4.1 Notification transport

Use a separate, versioned notification subject family so an old full-event consumer cannot mistake a hint for an event. A concrete proposed naming scheme is:

- Stream: `ORISUN_NOTIFICATIONS___<boundary>`.
- Subject: `ORISUN_NOTIFICATIONS___<boundary>.changed.v1`.
- Payload: empty; the subject carries boundary routing and protocol version.

Retain JetStream initially to reuse existing connection, authorization, replication, provisioning, and consumer lifecycle infrastructure. Do not add a simultaneous Core NATS delivery path in this change.

The stream can retain only the latest hint. Replaying a history of hints provides no additional recovery guarantee because recovery comes from the backend. Confirm stream limits and cluster replication explicitly in configuration tests. Notification size must be independent of event size and batch size.

Do not use a constant `Nats-Msg-Id`: JetStream deduplication must not suppress distinct future wake-ups. Duplicate hints are harmless and require no event-position-based deduplication ID.

Use an ephemeral consumer created before the initial backend drain, with `DeliverNew` semantics. An acknowledgement-free consumer is sufficient for hints: event delivery progress is tracked in the backend cursor, not in notification acknowledgements. If the existing integration requires explicit acknowledgements, acknowledge after recording the coalesced wake-up, and document that this does not acknowledge any event.

### 4.2 Boundary notification relay

Replace the event publisher with a boundary relay that:

1. Starts once per active boundary in a process.
2. Preserves the existing cluster lease/election where needed to avoid redundant relay work; lease loss cancels the relay. Duplicate publishers must still be harmless.
3. Registers its backend signal and emits an initial hint when it starts or takes ownership.
4. Waits for a backend signal, then publishes a small boundary hint.
5. Coalesces a burst of signals rather than publishing one message per event.
6. Handles transient publish failures with bounded, context-aware backoff, retaining pending work. It never makes a successful storage commit depend on NATS availability.
7. Stops its signal source, timers, and lease on exit.

Do not wait for event rows to become visible before sending a hint. A hint means “check,” not “every committed event is currently readable.” The subscriber's reconciliation timer handles visibility delays without moving its cursor past unseen events.

PostgreSQL reconnect should wake registered boundaries once after LISTEN is restored, because notifications during disconnection are not replayable. This is a latency improvement; subscriber reconciliation remains the correctness mechanism.

SQLite must signal only after the write transaction commits. Preserve the existing configurable coalescing delay unless separate measurements justify changing it. FoundationDB must preserve its transactional signal-key update and register watches without a read/watch race. Poll-only configurations can issue periodic hints without fetching event rows.

### 4.3 One subscription state machine

Extract the subscription drain and notification lifecycle into a focused server module rather than growing the existing monolithic method.

Proposed lifecycle:

```text
validate request and boundary
acquire subscriber-name lease
establish notification listener, or enter bounded polling-only operation
initialize event cursor according to the agreed start-position contract
request an initial drain

while subscription is active:
    wait for pending wake-up, reconciliation tick, or cancellation
    drain matching backend events after the delivery cursor
    verify strictly advancing positions before invoking handlers
    deliver sequentially
    update the delivery cursor after each successful handler call
    repeat reads until the backend reports no currently readable matches
```

Run notification reception separately from draining, but allow only the drain worker to own and mutate the event cursor. A capacity-one channel is sufficient for a pending wake-up. Drain the channel **before** starting work; never clear it after a read, since that could discard a notification that arrived during the read.

A notification arriving during a drain must either be covered by that drain or remain pending for another pass. An empty read is a statement about the current stable view, not proof that no future committed event can match.

The reconciliation timer must remain active when the notification iterator disconnects, fails, or is being recreated. Recreate the NATS consumer with bounded backoff and request an immediate drain after reconnect. Do not let a blocking NATS read prevent timer processing or cancellation. Bound notification setup attempts so initial catch-up is not indefinitely blocked by NATS.

Preserve the subscriber-name exclusivity and lease checks around reads and deliveries. A lease loss cancels further work. Backend failures must not advance the cursor: follow the existing retry/error policy, then terminate cleanly when appropriate so a client can resume from its own checkpoint. Retries must not hide invalid ordering or malformed backend batches.

### 4.4 Cursor semantics and exclusive reads

The subscription's supplied `after_position` is exclusive. Ordinary `GetEvents.from_position` is currently inclusive. Keep these distinct.

Add an explicit inclusive/exclusive cursor mode to the backend-owned read request, with inclusive as the existing default. Implement the mode in PostgreSQL, SQLite, and FoundationDB at their native position predicates. The subscription uses exclusive ascending reads; existing public `GetEvents` callers keep their behavior. This is a server-side read-contract extension, not a client protobuf change.

Do not implement exclusive reads by blindly incrementing `prepare_position`. Positions are ordered tuples, and prepare positions have backend-specific encodings. Avoid an inclusive batch-size-one loop that repeatedly reads and discards the same last event.

For a filtered subscription, retain the last **delivered matching event** as its cursor. Do not replace it with a notification position, a latest nonmatching event, or a batch maximum that was never delivered. An empty filtered result leaves that cursor unchanged. If a future optimization needs a separate scan frontier, the backend must return a proven stable frontier; do not invent one in subscription code.

**Start-position decision to settle before implementation:** characterize the existing omitted-position behavior and freeze its intended contract. The current implementation begins with a descending batch, delivers while mutating an ascending cursor, and can move the cursor backwards; that behavior must not be copied as a new contract. The recommended contract for an omitted position is “start now,” initialized from a backend-provided stable boundary watermark without delivering history. If supported callers instead rely on replaying the latest match, specify that behavior explicitly and test it. Do not silently change a supported API behavior. An explicit supplied position remains authoritative in either case.

If “start now” is selected, implement stable-watermark retrieval at the storage abstraction. In PostgreSQL, it must respect the same visibility barrier as streaming reads; a plain DESC maximum is insufficient. A sentinel for an empty boundary must allow the first later event to be read. Do not mix wall-clock timestamps with backend positions.

## 5. Ownership and code changes

### Subscription and operator work

- Replace `SubscribeToAllEvents`'s catch-up/live split with the state machine above.
- Delete timestamp lookups, the ten-second overlap, and `DeliverByStartTime` handoff logic.
- Remove full-event NATS envelope decoding and the live-only conversion helper.
- Remove `eventMatchesQueryCriteria` and its live-subscription tests.
- Keep query-shape validation. “No criteria matching in subscriptions” does not mean accepting unsupported operators.
- Audit `MatchTagValue`, `eventTagEquals`, and `orisun/tag_matching.go` before deleting or moving them: the FoundationDB backend now uses shared matching for storage reads. Preserve backend-owned functionality, and relocate it if necessary so its ownership is clear.
- Replace live-Go-matcher operator tests with subscription tests that prove the complete query is forwarded to the backend unchanged.

### Relay and runtime

- Replace `StartEventPolling` / `EventPollingManager` with a clearly named notification manager.
- Remove event batch reads, event serialization, per-event message IDs, and publisher cursor writes from its implementation.
- Update server startup, admin boundary bootstrap, dynamic boundary installation, embedding setup, and shutdown wiring.
- Create notification streams before relay startup and subscriber use. Unknown or inactive catalog boundaries must still fail early; do not reintroduce a startup boundary list.
- Preserve existing user edits in `server/server.go`, benchmarks, configuration documentation, and other dirty files.

### Publisher checkpoints

The durable event-publishing checkpoint becomes unnecessary once NATS carries hints and subscriptions independently reconcile against the backend.

- Remove its runtime requirement and live reads/writes.
- Inventory all implementations, exported embedding surfaces, admin metrics, tests, and benchmark fixtures referencing `EventPublishingTracker` before removing code.
- Leave existing checkpoint tables/keys intact on disk for rollback; do not add a destructive storage migration to this change.
- Remove obsolete provisioning and implementation code only after verifying it has no remaining owner. If an exported Go embedding API needs deprecation rather than immediate removal, document that source-compatibility decision explicitly; do not retain a second active publishing pipeline.
- Keep application/projector checkpoints. They record consumed progress and are not publisher checkpoints.

The repository's current guidance about per-event NATS publishing and durable publisher checkpoints describes the old architecture. Update that guidance as part of the implementation, once the replacement recovery guarantees are covered by tests. Preserve the backend stable-prefix and event-ordering requirements.

## 6. Configuration, deployment, and documentation

Introduce a backend-neutral subscription reconciliation interval, proposed as `ORISUN_SUBSCRIPTION_RECONCILE_INTERVAL`, defaulting to `1s`, validated as positive. Treat that default as a proposal to verify against idle-subscription database load. Inject the timer in tests rather than making tests sleep for production intervals. Allow bounded jitter in production so large groups of idle subscriptions do not query simultaneously; correctness must not depend on jitter.

Keep backend signaling settings, including PostgreSQL LISTEN enablement and SQLite wake-up coalescing. Audit `ORISUN_POLLING_PUBLISHER_BATCH_SIZE` and event-stream retention settings: they no longer control a full-event publisher. Document deprecation or replacement explicitly instead of silently giving them unrelated meanings. Prefer minimal notification configuration, with bounded retention, rather than copying every event-stream tuning knob.

This changes the internal NATS protocol. Use the separate notification namespace and plan a coordinated server upgrade for the first release. Do not claim that mixed old/new server nodes preserve notification behavior without an explicit interoperability test. There is no dual-publish compatibility mode in this plan.

Deployment procedure:

1. Record application/projector checkpoints and verify ordinary backend reads work.
2. Stop/drain old server subscription processes and their full-event publishers.
3. Deploy the server revision and provision notification streams for active boundaries.
4. Reconnect consumers using their durable backend positions; clients continue using the existing subscription RPC.
5. Verify admin projections, catalog activation, delivery latency, and reconciliation.
6. Retain old event streams and obsolete publisher checkpoint storage until the rollback window closes; remove them only in separately authorized cleanup.

Update product guidance under `docs/`, particularly API subscription behavior, delivery guarantees, positions, indexing, configuration, embedding, internals, and upgrade instructions. Explain that all delivery reads use backend criteria, that notifications can be duplicated or missed, and that existing SDKs need no subscription-contract change. Do not edit `README.md`.

## 7. Tests required for acceptance

Use deterministic coordination channels, fake clocks, and injected failures where possible. Fixed sleeps must not be the mechanism proving a race is safe.

| Test group | Required cases |
| --- | --- |
| Backend-owned matching | Query operators and repeated-key ranges are forwarded unchanged; returned rows are delivered without local JSON matching; each backend provides identical catch-up and post-notification matching behavior. |
| Initial handoff | Commit before listener setup, during setup, during initial read, after an empty read, and just before waiting. Each event eventually arrives without a timestamp overlap. |
| Notification loss | Drop all hints after a commit; the subscription's own timer still finds the event. Repeat with the relay stopped and with NATS unavailable. |
| Visibility gaps | PostgreSQL lower-position transaction remains in flight while later work commits; a hint can precede a stable visible result; later reconciliation delivers every event in order. |
| Duplicate/coalesced hints | Many commits produce one hint; duplicate hints produce no cursor regression or duplicate delivery within an uninterrupted successful session. |
| Draining and paging | Empty history, one row, exact batch boundary, multiple batches, batch size one, many nonmatching events, same-commit events, and backend-specific prepare-position boundaries. |
| Cursor ownership | Handler fails midway through a batch; cursor remains at the last successful handler. Notification position/sequence, nonmatching events, and failed reads never advance it. |
| Ordering defenses | A backend returns duplicate, stale, or descending rows; fail before delivering an invalid batch prefix and do not silently reorder it. |
| Slow handler | Hints arrive continuously while delivery blocks; memory remains bounded and pending work is not lost. |
| Reconnect | NATS connection loss, consumer recreation, retention expiry, stream recreation, and server restart. Resume from backend positions, not NATS history. |
| Lifecycle | Cancellation during setup/read/handler/wait, subscriber-name conflict, lease loss, dynamic boundary registration, inactive boundary, and bounded consumer cleanup. |
| Relay structure | Fake retriever/checkpoint tracker is absent from the relay; notifications contain no event IDs, data, metadata, or serialized events; publish failure does not affect committed writes. |
| Backend signals | PostgreSQL reconnect and dynamic LISTEN routing; SQLite notify-after-commit and coalescing; FoundationDB watch re-arming; polling-only operation. |
| Application services | Admin user/auth projections, event counts, catalog/bootstrap, and dynamically activated boundaries continue progressing with hints and during notification outages. |
| Upgrade constraints | Old and new subject namespaces do not overlap; checkpoint-based gRPC resume works with unchanged clients; retained old checkpoint tables do not affect the new path. |

Retain storage-level operator, CCC, expected-position, and stable-prefix tests. Publisher tests that assert event payload serialization or per-event NATS message counts should be replaced, not weakened to pass with meaningless hints.

Run focused packages first, then integration suites:

```bash
go test ./orisun ./orisun/grpcapi ./internal/... ./eventstore
go test ./sqlite
go test ./postgres
./scripts/fdb_test_container.sh -run TestFoundationDB -count=1
go test ./server ./cmd
go test -race ./orisun ./sqlite
go test ./...
./scripts/verify_embedded_dependencies.sh
cd docs && bun run build
```

PostgreSQL and FoundationDB tests require their normal Docker/native test environments. Keep full logs for integration failures and distinguish an environment startup failure from a delivery invariant failure. Do not run client generation or client builds as part of this server-only work.

## 8. Performance and observability

Expected benefit: one hint can cover many events, the relay stops fetching and serializing payloads, large events no longer encounter the NATS event-payload limit, and publisher checkpoint writes disappear from the hot path.

Expected cost: each subscription reads its own matching data from the backend. More subscribers, broad filters, and reconciliation ticks increase read load. FoundationDB queries still need the appropriate native range or ready covering index. Preserve per-subscription backpressure and measure pool contention; do not bypass backend query restrictions to make live delivery work.

Measure before and after with the same payloads, indexes, subscriber counts, write rate, and durability settings:

- Backend read queries/second and rows/bytes read per delivered event.
- Notification messages/second and bytes/second.
- Commit-to-handler latency (p50/p95/p99), including notification-outage latency.
- Idle subscription polling cost and read-pool wait time.
- Relay and subscription CPU, allocations, memory, and cancellation time.
- High-fanout, selective-filter, broad-range, large-event, and slow-consumer workloads.
- Write throughput under active subscription load, not just an idle system.

Add counters for received/coalesced hints, reconciliation drains, delivered events, empty reads, reconnects, read failures, and lease loss. Do not label metrics with raw criteria values, event payloads, or unbounded subscriber names. Report lag from backend positions or delivered event timestamps, never from a notification timestamp alone.

## 9. Implementation order and completion checklist

1. **Freeze semantics:** characterize omitted positions; select and document their contract; define exclusive cursor mode, stable-read behavior, signal lifecycle, and notification namespace.
2. **Implement backend cursor support:** add exclusive reads and, if required, stable-watermark retrieval across all three backends. Test these contracts independently.
3. **Implement the subscription drain:** one backend reader, one delivery cursor, sequential handlers, coalesced wake-ups, independent reconciliation, and lease/cancellation handling.
4. **Implement the notification relay:** no event retrieval or checkpoint dependency; after-commit signals, publish failure handling, startup/reconnect hints, and dynamic boundaries.
5. **Switch runtime wiring atomically:** replace the live pipeline, provision isolated notification streams, and remove the old event-delivery path. Keep checkpoints on disk only for rollback.
6. **Reconcile operator code and tests:** remove subscription matching while retaining storage-owned evaluators; add end-to-end filtered subscription coverage.
7. **Verify services and failures:** run lifecycle, gap, reconnect, restart, admin projection, and backend integration tests; check dependency isolation.
8. **Measure and document:** evaluate read amplification and latency, settle interval defaults, update operational/configuration/architecture guidance, and validate the documentation build.

The change is complete when every delivered subscription event comes from a backend read, no subscription evaluates criteria locally, the relay publishes only boundary hints without reading event batches, missed hints recover automatically, event order and cursor ownership are proven by tests, existing clients remain untouched, and the upgrade procedure is reviewable.
