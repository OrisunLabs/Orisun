---
id: internals
title: Internals
description: How Orisun composes boundaries, evaluates CCC writes, orders events, and delivers them without skips.
slug: /internals
---

This page describes the mechanisms behind Orisun's public guarantees. It is
intended for operators and contributors who need to reason about concurrency,
failure, and ownership across the PostgreSQL, SQLite, and FoundationDB
backends.

For the public contracts, start with
[Command Context Consistency](./concepts/command-context-consistency),
[Positions](./concepts/positions), [Indexing](./concepts/indexing), and
[Delivery Guarantees](./concepts/delivery-guarantees).

## The system at a glance

```text
gRPC or embedded API
        |
        v
transport adapter + EventStore core
        |
        v
active-boundary gate + backend-neutral ports
        |
        +----------------------+
        |                      |
        v                      v
durable backend log      boundary catalog in
and checkpoints          the admin boundary
        |                      |
        v                      v
boundary signal relay    provisioning + local
        |                 runtime installation
        v
embedded JetStream
        |
        v
backend-draining subscriptions and projectors
```

The PostgreSQL, SQLite, or FoundationDB event log is the durable source of
truth. Core NATS carries empty boundary hints. Every subscription event is read from the backend. Process-local registries,
activation gates, listeners, and caches are rebuilt or repopulated state; they
must not be required to recover durable data.

Several invariants shape the implementation:

- A CCC check and its append are one atomic backend operation.
- An active boundary is defined by the durable catalog, then installed into
  each process before that process accepts requests for it.
- Positions are totally ordered within a boundary. Their encoding is
  backend-specific and should be treated as opaque.
- Each subscription reads a stable committed prefix and delivers matching events
  in ascending position order through one reader.
- Delivery is at least once. Applications persist their own successful progress;
  subscription idle watchdogs publish NATS hints to recover missed signals.
- Wake-up signals improve latency; backend cursors and repeated reads provide
  recovery.

## Runtime composition

`server.Run` is the shared composition root. A backend initializer supplies
narrow implementations for saving, reading, locking, admin state, publisher
checkpoints, wake-up signals, and boundary provisioning. The server then wires
those ports into:

1. the embedded NATS and JetStream runtime;
2. the transport-neutral EventStore core;
3. the admin boundary and boundary lifecycle slices;
4. one publisher contender per locally installed boundary;
5. internal projectors and catch-up subscriptions; and
6. the gRPC transport.

The backend-specific binaries and embedding packages select the initializer.
`cmd/orisun-pg` and `embedded/postgres` do not depend on SQLite, and the
equivalent SQLite and FoundationDB entry points remain backend-specific. The
generated protobuf messages and gRPC adapters live under `orisun/grpcapi`;
domain slices and storage packages exchange transport-neutral values.

## Boundaries are catalog-driven

A boundary is both a durable logical definition and a locally installed
runtime resource. Creation deliberately separates those two concerns.

### Creation and activation

1. `CreateBoundary` validates the immutable placement and appends one
   `BoundaryCreated` event to the admin boundary using CCC.
2. The request returns the event-rebuilt definition in `PROVISIONING`.
   Physical DDL, files, or key ranges are not created in the RPC transaction.
3. Every server establishes the same exclusive `boundary-provisioning`
   catch-up subscription. Its distributed subscription lease elects one active
   provisioning controller.
4. That controller validates and initializes storage. Notification
   subjects require no provisioning. It then appends either `BoundaryActivated` or
   `BoundaryProvisioningFailed`.
5. Every process also owns a uniquely named runtime subscription. On an
   activation event it installs the boundary into that process's backend
   registry and signal provider, starts the local publisher contender and
   dynamic projectors, and only then opens the local request gate.
6. `ListBoundaries` and `GetBoundary` rebuild catalog state from the durable
   lifecycle events.

Provisioning and installation are idempotent because either can be retried
after a crash or partial attempt. A replacement controller replays the catalog
while holding the same subscription lease. Active definitions do not repeat
physical provisioning, but their streams are re-ensured.

Startup replays activation state before gRPC is exposed. Live subscription
gaps cause another replay. A boundary may therefore be durably `ACTIVE` before
a particular process has completed its local installation; that process
continues to reject application requests for the boundary until installation
finishes.

Unknown, provisioning, failed, and not-yet-installed boundaries fail at the
active-boundary gate before the storage adapter is called. Application
boundaries come only from the catalog; there is no independent startup
boundary list.

### Physical placement

| Backend | Boundary placement |
| --- | --- |
| PostgreSQL | A catalog placement selects a schema; boundary tables and functions are prefixed within it. |
| SQLite | A boundary maps to its event database and metadata database files. |
| FoundationDB | A boundary maps to tuple-encoded key ranges under the configured Orisun root. |

The admin boundary is bootstrapped so it can contain the catalog that activates
all other boundaries. Existing storage must use the current schema. Startup
rejects older formats and installs active placements from catalog definitions.

## `SaveEventsV2`: one contract, three concurrency models

Before invoking a backend, the EventStore core validates the request, converts
events into an immutable prepared batch, and checks the local active-boundary
gate.

Each consistency observation owns one complete OR query and one position. The
backend finds the latest event matching every observation's query and compares
its complete position with the observation position. A missing match is the
empty position. If any comparison differs, Orisun returns `ALREADY_EXISTS`
and writes none of that request's events. An empty observation list is an
unconditional append.

The check and append must remain atomic. Performing the query first and the
insert in a later transaction would allow a conflicting event to commit
between them.

| Backend | Save execution | Boundary-wide serialization |
| --- | --- | --- |
| PostgreSQL | Per-boundary in-process group-commit queue; one SQL transaction per flush | A transaction-scoped PostgreSQL advisory lock orders all writers across processes |
| SQLite | Per-boundary in-process group-commit queue; one `BEGIN IMMEDIATE` transaction per flush | SQLite's single writer for the boundary file |
| FoundationDB | One native FoundationDB transaction per `SaveEventsV2` request | None for plain appends; CCC conflicts are scoped by native or secondary-index ranges |

### PostgreSQL group commit

Concurrent requests enter a bounded queue for their boundary. A worker drains
an opportunistic batch, excluding requests whose contexts were already
cancelled, and executes the remaining requests through one database call and
one outer transaction. The flush uses its own timeout context so cancellation
of one caller cannot interrupt unrelated requests in the same batch.

Every flush uses `insert_event_requests_v2`, including batches of one,
unconditional writes, and all CCC query shapes. The function takes a
transaction-scoped advisory lock keyed by schema and boundary, held from the
CCC state read and position draw through commit. This orders writers across
processes.

The batcher validates request envelopes and consistency-query structure before
building the SQL batch. Malformed requests receive individual errors and never
participate in criterion state or position allocation.

The SQL function deduplicates criteria and groups them once by key shape. It
resolves each criterion's latest persisted position with literal predicates and
`ORDER BY ... LIMIT 1`, then evaluates requests in queue order. Accepted requests
project their final event documents onto each distinct criterion key shape and
look up the matching criterion ID directly. This avoids rebuilding a SQL join
against every criterion for each accepted request. Events advance criterion
state in order, so later requests observe earlier accepted writes, including
queries on store-owned envelope fields. A CCC conflict rejects only that request.
Accepted events and their write contexts are bulk-inserted together.

An error that aborts the PostgreSQL statement or transaction rolls back all
writes in that SQL batch. Examples include a database constraint violation, a
failed numeric cast in an index expression, statement cancellation due to a
timeout, a deadlock, or an unexpected exception from a database function. These
errors differ from a CCC conflict, which rejects only the affected request with
`ALREADY_EXISTS`, and invalid requests caught by Go validation, which are excluded
before the SQL batch executes.

A client-side error does not always prove rollback. If the connection drops
while PostgreSQL commits, the client may not know whether the batch committed.

Group-commit settings control batching and waiting time; they do not select a
different persistence implementation.

### SQLite group commit

SQLite uses the same queue-per-boundary shape, but executes directly on the
boundary's single write connection. One opportunistically drained flush owns a
`BEGIN IMMEDIATE` transaction. Every request, including unconditional writes
and batches of one, uses the same queue-ordered check and insert implementation
inside a savepoint:

- an accepted request remains visible to later CCC checks in queue order;
- a CCC or validation failure rolls back only that request and consumes no
  positions; and
- a failure to begin, update the sequence, or commit the outer transaction
  fails the whole flush.

Each CCC observation is an OR of AND criteria. Redundant criteria are removed
(`A OR (A AND B)` is equivalent to `A`). Each remaining criterion uses its own
literal, ordered `LIMIT 1` lookup, allowing a matching partial or expression
index to find the latest event without sorting the combined matching history.
The maximum full `(transaction_id, global_id)` position across those lookups is
compared with the observation's expected position.

The transaction reads the sequence once and advances its local cursor only
after a request's savepoint succeeds. It persists the final sequence once,
atomically with all accepted writes. Each event's stored envelope is constructed
once, after its positions are assigned, preserving application JSON numbers.

The event log and metadata use separate databases for each boundary. SQLite is
a single-node backend, and startup rejects configurations that enable NATS
clustering with SQLite.

### FoundationDB transactions

FoundationDB does not use the process-local group-commit queues. Each
`SaveEventsV2` call executes as one FoundationDB transaction:

- criteria reads and event writes share the transaction;
- criteria require a native range anchored by `__commitPosition` or
  `__writeId`, or a ready covering secondary index; unsupported criteria fail
  with `FAILED_PRECONDITION`;
- the transaction reads the index epoch so an index definition change forces
  an overlapping save to retry with the current index set;
- matching native event or secondary-index ranges provide CCC conflict coverage, allowing
  unrelated contexts in one boundary to commit concurrently;
- events and their index entries are written with commit versionstamps; and
- the estimated payload and index footprint is checked before commit to stay
  within FoundationDB's transaction budget.

The backend maps a failed CCC comparison to `ALREADY_EXISTS`. FoundationDB's
normal transaction retry behavior handles retryable storage conflicts before a
result is returned.

FoundationDB support is beta. See
[FoundationDB Operations](./operations/foundationdb) for its deployment and
release constraints.

### Cancellation and unknown outcomes

Cancellation before a queued request is included in a flush excludes it.
Cancellation after inclusion has the same ambiguity as cancelling any database
write during commit: the caller can stop waiting while the transaction still
commits. A connection failure at commit can also leave the result unknown.
Retries should therefore follow the guidance in
[Idempotency and Retry](./patterns/idempotency-and-retry).

## Positions and stable-prefix reads

The public position is the lexicographically ordered pair
`(commit_position, prepare_position)`. It is a boundary-local ordering token,
not a portable database sequence.

| Backend | Position construction |
| --- | --- |
| PostgreSQL | `global_id` is a boundary sequence. Each accepted request receives a logical `transaction_id` derived from the last sequence value in that request, even when several requests share one physical group-commit transaction. |
| SQLite | Boundary-local counters assign the logical transaction and event order while the single writer holds the transaction. |
| FoundationDB | The commit versionstamp supplies commit order; the versionstamp batch component and event offset supply order within a commit. |

### PostgreSQL's visibility barrier

PostgreSQL stores an additional `pg_xact_id` beside the public logical
position. It is an internal, current-cluster visibility marker and is not
returned to clients. Multiple requests in one group flush can share that
physical XID while retaining distinct public positions.

Ascending reads add this stable-prefix predicate:

```sql
pg_xact_id IS NULL
OR pg_xact_id::TEXT::xid8 < pg_snapshot_xmin(pg_current_snapshot())
```

It prevents an ascending reader from passing an in-flight transaction and
returning a later committed position first. Rows restored from a dump may have
a null marker because an XID is not meaningful across clusters; those rows are
already durable and are safe to read.

The publisher depends on this barrier. `LISTEN/NOTIFY` can wake it, but a
wake-up cannot prove that every earlier transaction is visible.

### Read batches

Backend reads return packed, contiguous event batches with value positions and
timestamps. Internal subscriptions and projectors can consume those values
without constructing a protobuf object graph for every row. The gRPC adapter
materializes generated response objects only at the transport boundary.

Read pages are capped at 10,000 events. Internal drainers advance by the last
position and continue across pages.

## Content-query indexes

Indexes are explicit, boundary-scoped resources managed through the EventStore
index API:

- PostgreSQL creates targeted JSON expression indexes concurrently and records
  their definitions in boundary metadata.
- SQLite creates targeted expression indexes in the boundary event database
  and records their definitions in metadata.
- FoundationDB backfills versioned index key ranges and exposes
  `BUILDING`/`READY` state.

PostgreSQL and SQLite preserve correctness without a matching user index, but a
CCC check or read may scan the boundary event table. Orisun does not create a
broad automatic GIN index. FoundationDB instead fails closed when criteria are
unable to select a native position range or a ready covering index; a boundary scan inside a transaction would be
both unsafe for scale and too broad for useful conflict isolation.

The PostgreSQL criterion-state group-commit path builds shape-specific,
indexable predicates. Production workloads should still create indexes for the
fields used by their CCC contexts and reads. See [Indexing](./concepts/indexing)
and the [`CreateIndex` API](./api/eventstore#createindex).

## Boundary notification relays

The notification manager starts one local relay contender per installed
boundary. PostgreSQL and SQLite runtimes use revision-fenced JetStream KV
leases; FoundationDB uses a token-fenced renewable lease in FoundationDB.
SQLite still permits only one Orisun node. The PostgreSQL advisory write lock
serializes position assignment and is separate from relay ownership.

A relay registers its backend signal, emits an initial empty hint, then forwards
coalesced signals to `ORISUN_NOTIFICATIONS___<boundary>.changed.v1`. It checks
its lease before publishing and retains pending work during context-aware,
bounded publish backoff. It has no event retriever or checkpoint dependency.
A relay never makes a successful durable write depend on NATS availability.

| Backend | Relay wake-up |
| --- | --- |
| PostgreSQL | Boundary-specific `LISTEN/NOTIFY`, plus a wake after reconnect |
| SQLite | In-process coalesced notification after commit |
| FoundationDB | Watch on a transactionally updated boundary signal key |
| Backend notifications disabled | No relay signal; subscription idle watchdogs publish NATS hints |

## Backend-driven subscriptions

A subscription validates its boundary and query, acquires its subscriber-name
lease, and registers a Core NATS listener before its initial read. Registration
is flushed with a bounded deadline; failures retry listener
registration with bounded backoff. Hints record no event progress. NATS restores
listeners on reconnect. After registration and every reconnect, publish a
boundary hint; only receiving it wakes the backend drain.

One reader owns the event cursor and all handler calls. It passes the complete
query to the backend and requests inclusive ascending batches of 100. Only the
first row equal to the cursor is discarded; every remaining position must
strictly advance. The entire batch is validated before delivering any prefix.
The initial omitted-position lookup selects one latest match descending, then
switches permanently to ascending reads. An empty initial result leaves a nil
cursor for subsequent ascending reads.

The cursor advances only after a successful handler call. A failed handler or
backend read cannot advance past an undelivered event. Lease and active-boundary
checks run before reads and deliveries. Cancellation stops notification
reception, the idle watchdog timer, listener cleanup, and the lease.

A capacity-one channel records pending wake-ups. The reader consumes a wake-up
before its read, so a hint arriving during a drain remains pending for another
pass. A per-subscription idle watchdog tracks the last received hint and publishes
an empty boundary hint after `ORISUN_SUBSCRIPTION_IDLE_THRESHOLD`. Receiving that
hint wakes the drain; publishing it never directly reads the backend. Initial
setup and NATS reconnect publish the same hint to request a drain. Healthy NATS is required for
live delivery. There is no periodic backend polling, timestamp handoff, or local
payload matcher.

Handlers receive transport-neutral events synchronously and provide
backpressure. A successful gRPC send is not an application transaction
acknowledgement. Consumers persist their own checkpoint after durable side
effects and deduplicate replay after reconnecting or crashing. This remains
at-least-once delivery.

## Durable and disposable state

| State | Durable home | Scope and recovery |
| --- | --- | --- |
| Events | Selected backend | Source of truth, partitioned by boundary |
| Boundary catalog | Admin boundary event log | Replayed to recover lifecycle state |
| Index definitions and build state | Selected backend | Boundary-scoped; physical indexes or key ranges are reconciled from it |
| Projector checkpoints and projections | Selected backend | Rebuilt or resumed from durable events |
| Core NATS notification listener | Process memory | One pending hint; recovery comes from subscription backend reads |
| Active-boundary gate | Process memory | Rebuilt from activation replay before requests are admitted |
| Backend boundary registry and signal listeners | Process memory | Reinstalled from the catalog on every process |
| User lookup and other hot-path caches | Process memory | Disposable accelerators; durable admin state remains authoritative |
| Wake-up notifications | PostgreSQL, process channels, or FDB watches | Ephemeral hints backed by a NATS idle watchdog |

This separation is intentional: a process may lose every local cache and
listener, or NATS may drop a hint, without losing the
durable event history or advancing a checkpoint incorrectly.

## Failure semantics

| Failure | Result |
| --- | --- |
| Any CCC observation no longer equals the latest position of its query | That request returns `ALREADY_EXISTS`; none of its events are appended |
| Request-local error inside a multi-request group flush | That request rolls back; later requests continue in queue order |
| Known outer transaction rollback | No accepted request in that transaction persists |
| Caller cancellation or connection loss around commit | Outcome may be unknown; retry idempotently |
| Subscription crash before application checkpoint | Resume from the durable application checkpoint; events can be delivered again |
| Lost wake-up | The subscription idle watchdog publishes a NATS hint; receipt resumes the backend drain while NATS is healthy |
| Relay lease loss | The owner stops forwarding hints; a successor acquires the lease |
| Process-local registry or cache loss | Startup replay and backend reads rebuild disposable state |
| Unknown or non-active boundary | Rejected before the request reaches backend storage |

## Package map

| Package | Responsibility |
| --- | --- |
| `server/` | Backend-neutral runtime composition, lifecycle wiring, projectors, and gRPC hosting |
| `orisun/` | EventStore core, reads, subscriptions, notification relays, locks, and public domain values |
| `boundary/` and `admin/slices/` | Event-backed catalog model and use-case slices |
| `postgres/` | PostgreSQL storage, group commit, migrations, indexing, checkpoints, and notifications |
| `sqlite/` | SQLite storage, group commit, per-boundary files, indexing, checkpoints, and signals |
| `foundationdb/` | FoundationDB transactions, versionstamped layout, covering indexes, watches, and leases |
| `nats/` | Embedded NATS and JetStream lifecycle |
| `orisun/grpcapi/` | Generated protobuf code and domain-to-transport adapters |
| `cmd/` and `embedded/` | Executable and in-process composition roots |

Hot-path PostgreSQL SQL strings are precomputed when a boundary is installed,
and boundary lookup is a registry map read rather than per-call identifier
formatting. Go runtime CPU limits are detected through `automaxprocs`;
`GOMEMLIMIT` and `GOGC` retain their standard Go meanings. See
[Configuration](./operations/configuration) and
[Observability](./operations/observability) for operational controls.
