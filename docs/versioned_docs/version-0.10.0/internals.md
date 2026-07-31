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
checkpointed publisher   provisioning + local
        |                 runtime installation
        v
embedded JetStream
        |
        v
catch-up/live subscriptions and projectors
```

The PostgreSQL, SQLite, or FoundationDB event log is the durable source of
truth. JetStream is an in-memory live-delivery buffer. Process-local registries,
activation gates, listeners, and caches are rebuilt or repopulated state; they
must not be required to recover durable data.

Several invariants shape the implementation:

- A CCC check and its append are one atomic backend operation.
- An active boundary is defined by the durable catalog, then installed into
  each process before that process accepts requests for it.
- Positions are totally ordered within a boundary. Their encoding is
  backend-specific and should be treated as opaque.
- A publisher reads a stable committed prefix and never publishes a later
  event ahead of an earlier event in the same boundary.
- Publishing is at least once. A crash can cause duplicates, but the durable
  checkpoint prevents skips.
- Wake-up signals improve latency; checkpoints and repeated reads provide
  correctness.

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
4. That controller provisions storage, applies migrations, and ensures the
   boundary's JetStream stream. It then appends either `BoundaryActivated` or
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
all other boundaries. PostgreSQL and SQLite can migrate supported pre-catalog
storage through the normal command path. FoundationDB is beta and has no
legacy catalog-discovery path.

## `SaveEvents`: one contract, three concurrency models

Before invoking a backend, the EventStore core validates the request, converts
events into an immutable prepared batch, and checks the local active-boundary
gate.

When a request carries consistency criteria, the backend finds the latest event
matching that content query and compares its complete position with
`expected_position`. A missing match is the empty position. If the positions
differ, Orisun returns `ALREADY_EXISTS` and writes none of that request's
events. If the request has no criteria, `expected_position` alone does not
create a consistency context.

The check and append must remain atomic. Performing the query first and the
insert in a later transaction would allow a conflicting event to commit
between them.

| Backend | Save execution | Boundary-wide serialization |
| --- | --- | --- |
| PostgreSQL | Per-boundary in-process group-commit queue; one SQL transaction per flush | A transaction-scoped PostgreSQL advisory lock orders all writers across processes |
| SQLite | Per-boundary in-process group-commit queue; one `BEGIN IMMEDIATE` transaction per flush | SQLite's single writer for the boundary file |
| FoundationDB | One native FoundationDB transaction per `SaveEvents` request | None for plain appends; CCC conflicts are scoped by indexed reads |

### PostgreSQL group commit

Concurrent requests enter a bounded queue for their boundary. A worker drains
an opportunistic batch, excluding requests whose contexts were already
cancelled, and executes the remaining requests through one database call and
one outer transaction. The flush uses its own timeout context so cancellation
of one caller cannot interrupt unrelated requests in the same batch.

Every PostgreSQL path takes the same transaction-scoped advisory lock keyed by
the schema and boundary. It is held from the CCC state read and position draw
through commit. This is the cross-process serialization point: group commit
reduces transaction and round-trip overhead within one process without
weakening ordering between processes.

The batcher selects the narrowest safe SQL path:

| Path | Eligible requests | Execution strategy |
| --- | --- | --- |
| Unconditional | Canonical event batches with no CCC query | Assign positions and insert all events set-wise |
| Independent CCC | One equality tag on the same key, unique context values, and events contained in their own contexts | Check one locked snapshot and bulk-insert accepted requests |
| Criterion state | Canonical general AND/OR criteria | Deduplicate criterion shapes, load their latest state, evaluate requests in queue order, then bulk insert |
| Isolated fallback | Requests that can raise request-local validation errors or do not fit the optimized shapes | Run each request in a PL/pgSQL subtransaction |

Later requests in a flush observe earlier accepted writes in that flush.
A CCC conflict or request-local validation failure rejects only that request;
it does not poison later requests. A failure of the outer transaction fails all
requests that did not already have an isolated result.

The single-request primitive remains
`insert_events_with_consistency_v3`. Group flushes enter through
`insert_unconditional_event_requests_v1`,
`insert_independent_event_requests_with_consistency_v1`,
`insert_canonical_event_requests_with_consistency_v1`, or
`insert_event_requests_with_consistency_v1`.

### SQLite group commit

SQLite uses the same queue-per-boundary shape, but executes directly on the
boundary's single write connection. One opportunistically drained flush owns a
`BEGIN IMMEDIATE` transaction. In a multi-request flush, each request runs
inside a savepoint:

- an accepted request remains visible to later CCC checks in queue order;
- a CCC or validation failure rolls back only that request, including its
  sequence update; and
- a failure to begin or commit the outer transaction fails the whole flush.

The event log and metadata use separate databases for each boundary. SQLite is
a single-node backend, and startup rejects configurations that enable NATS
clustering with SQLite.

### FoundationDB transactions

FoundationDB does not use the process-local group-commit queues. Each
`SaveEvents` call executes as one FoundationDB transaction:

- criteria reads and event writes share the transaction;
- criteria require ready covering indexes and fail with
  `FAILED_PRECONDITION` when no suitable index exists;
- the transaction reads the index epoch so an index definition change forces
  an overlapping save to retry with the current index set;
- matching index ranges provide the conflict ranges for CCC, allowing
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
not covered by a ready index; a boundary scan inside a transaction would be
both unsafe for scale and too broad for useful conflict isolation.

The PostgreSQL criterion-state group-commit path builds shape-specific,
indexable predicates. Production workloads should still create indexes for the
fields used by their CCC contexts and reads. See [Indexing](./concepts/indexing)
and the [`CreateIndex` API](./api/eventstore#createindex).

## Publishing: durable checkpoint, ephemeral wake-up

The polling manager starts one local publisher contender for each installed
boundary. A distributed lease selects one active publisher for that boundary:

- PostgreSQL and SQLite runtimes use the revision-fenced JetStream KV lease
  provider. SQLite still runs only one Orisun node.
- FoundationDB uses a token-fenced, renewable lease stored in FoundationDB.

The PostgreSQL advisory lock described in the write path is not publisher
ownership; it only serializes write-position assignment.

After acquiring its lease, the publisher:

1. loads the durable per-boundary checkpoint;
2. reads events after it in ascending position order;
3. verifies that the entire fetched batch is strictly advancing before
   publishing any of it;
4. publishes events sequentially and waits for each JetStream acknowledgement;
5. writes the final batch position to the durable backend; and
6. repeats until drained, then waits for a signal or polling interval.

The ownership lease is checked before publishing and before advancing the
checkpoint. Per-boundary work is sequential by position, so a publisher never
publishes a partial valid prefix followed by an invalid or non-advancing tail.

### Why delivery is at least once

JetStream publish and backend checkpoint storage are separate operations. If a
publisher crashes after JetStream accepts an event but before the checkpoint
commits, its successor resumes from the older checkpoint and publishes that
event again. This produces a duplicate rather than a gap. Consumers should
deduplicate by `event_id`.

The checkpoint is advanced only after every event in the batch is
acknowledged. A publish failure therefore never records progress past an
unpublished event.

### Signals are latency hints

| Backend | Publisher wake-up |
| --- | --- |
| PostgreSQL | Boundary-specific `LISTEN/NOTIFY` |
| SQLite | In-process coalesced notification |
| FoundationDB | Watch on a boundary signal key |

Every backend also falls back to polling. Losing or coalescing a signal can
delay a read, but cannot lose an event because the next pass starts from the
durable checkpoint.

## Catch-up subscriptions and live handover

`CatchUpSubscribeToEvents` first reads from the durable backend after the
requested position, then attaches to the boundary's JetStream stream for live
events. JetStream retention must cover the short catch-up-to-live handover
window. A subscriber farther behind than the in-memory retention window still
recovers from the durable log.

The full lifetime of a named subscription is protected by the same lock
abstraction used for publisher ownership. Lease values contain a unique owner
token and renewable expiry. Revision-fenced acquire, renew, and release
operations prevent an expired owner from deleting or renewing a successor's
lease.

Internal subscribers receive transport-neutral event values through a
synchronous callback. Callback completion provides backpressure; a callback
error terminates the subscription. The gRPC streaming method adapts this path
without moving generated message types into the core.

Consumers can still see duplicates at restart, handover, or the
publish/checkpoint boundary. They must not infer exactly-once delivery from the
single-active-subscriber lease.

## Durable and disposable state

| State | Durable home | Scope and recovery |
| --- | --- | --- |
| Events | Selected backend | Source of truth, partitioned by boundary |
| Boundary catalog | Admin boundary event log | Replayed to recover lifecycle state |
| Index definitions and build state | Selected backend | Boundary-scoped; physical indexes or key ranges are reconciled from it |
| Publisher checkpoint | Selected backend | Resumes delivery after restart or ownership change |
| Projector checkpoints and projections | Selected backend | Rebuilt or resumed from durable events |
| JetStream event stream | Embedded NATS memory | Bounded live-delivery buffer, never the durable source |
| Active-boundary gate | Process memory | Rebuilt from activation replay before requests are admitted |
| Backend boundary registry and signal listeners | Process memory | Reinstalled from the catalog on every process |
| User lookup and other hot-path caches | Process memory | Disposable accelerators; durable admin state remains authoritative |
| Wake-up notifications | PostgreSQL, process channels, or FDB watches | Ephemeral hints backed by polling |

This separation is intentional: a process may lose every local cache and
listener, or JetStream may replay an acknowledged event, without losing the
durable event history or advancing a checkpoint incorrectly.

## Failure semantics

| Failure | Result |
| --- | --- |
| CCC context no longer equals `expected_position` | That request returns `ALREADY_EXISTS`; none of its events are appended |
| Request-local error inside a multi-request group flush | That request rolls back; later requests continue in queue order |
| Known outer transaction rollback | No accepted request in that transaction persists |
| Caller cancellation or connection loss around commit | Outcome may be unknown; retry idempotently |
| Publisher crash before checkpoint | Already accepted JetStream messages can be delivered again |
| Lost wake-up | The fallback poll reads from the durable checkpoint |
| Publisher lease loss | The owner stops before further publish/checkpoint work; a successor resumes |
| Process-local registry or cache loss | Startup replay and backend reads rebuild disposable state |
| Unknown or non-active boundary | Rejected before the request reaches backend storage |

## Package map

| Package | Responsibility |
| --- | --- |
| `server/` | Backend-neutral runtime composition, lifecycle wiring, projectors, and gRPC hosting |
| `orisun/` | EventStore core, reads, subscriptions, publisher loop, locks, and public domain values |
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
