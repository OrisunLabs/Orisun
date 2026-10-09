---
title: Storage Backends
description: Choose between PostgreSQL, SQLite, and FoundationDB deployment profiles.
---

Orisun supports PostgreSQL, SQLite, and FoundationDB. The backend is selected
with `ORISUN_BACKEND` or by using a backend-specific binary or Docker image.

FoundationDB support is beta. It is tested for correctness and failover, but storage layout, index internals, and operational contracts may still receive breaking changes while the backend hardens.

## Backend Matrix

| Backend | Use case | Multi-node | Driver |
| --- | --- | --- | --- |
| `postgres` | Production clusters, larger datasets, shared database platforms | Yes | `pgx` |
| `sqlite` | Embedded apps, edge, development, low-ops single-node production | No | `zombiezen.com/go/sqlite` |
| `foundationdb` | Distributed transactional key-value deployments | Yes | FoundationDB Go binding; beta |

## PostgreSQL

PostgreSQL is the clustered backend. Multiple Orisun nodes can share one database. Notification relays coordinate through renewable JetStream leases so one active relay owns a boundary at a time. Writes also take a short per-boundary advisory lock while drawing public positions and committing, which preserves commit-ordered positions across concurrent writers.

PostgreSQL stores:

- event log tables
- per-boundary position state
- projector checkpoints
- index metadata
- admin state

Choose PostgreSQL when you need horizontal Orisun nodes, database-managed backup/restore, mature operational tooling, or PgBouncer integration.

The write lock is per boundary, not global. It is intentional: Command Context Consistency relies on public positions being a stable upper bound for committed events in that boundary. Split unrelated high-write domains into separate boundaries when they do not share invariants.

### PostgreSQL position metadata

PostgreSQL mode stores two ordering-related values:

- `transaction_id`: Orisun's logical commit position, durable across PostgreSQL major upgrades and restore workflows.
- `pg_xact_id`: PostgreSQL's internal transaction ID, used only as a current-cluster visibility marker so ascending subscription reads do not skip older open transactions.

Do not use PostgreSQL internal transaction IDs as application cursors. Older
storage formats before `0.13.0` are rejected. See [Positions and Ordering](./positions).

## FoundationDB

FoundationDB is a beta clustered backend built on ordered key-value transactions. Use the `orisun-fdb` release binary, the `orisunlabs/orisun:fdb` / `ghcr.io/orisunlabs/orisun:fdb` image, or build your own binary with the `foundationdb` build tag and native FoundationDB client libraries.

FoundationDB stores:

- event records in ordered per-boundary key ranges
- projector checkpoints
- admin state
- index metadata and secondary index keys
- watch signal keys for notification relay wake-ups
- token-fenced notification relay lease locks

```bash
go build -tags foundationdb ./cmd/orisun-fdb

ORISUN_BACKEND=foundationdb
ORISUN_FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ORISUN_FDB_ROOT=orisun
```

Criteria queries keep the same public API. FoundationDB requires each criterion
to select a native range through `__commitPosition` or `__writeId`, or have a
ready covering secondary index. This applies to reads and every `SaveEventsV2`
observation. Unsupported criteria fail with `FAILED_PRECONDITION`, avoiding
boundary-wide scans and keeping conflict ranges scoped to the selected events.

FoundationDB assigns event positions with commit versionstamps instead of a per-boundary counter. Plain appends can commit in parallel; writes with consistency observations track conflicts on the native event or secondary-index ranges selected by those queries, so commands on unrelated contexts in one boundary commit concurrently.

For cluster layout, process classes, Kubernetes, backups, monitoring, lock failover, and release gates, see [FoundationDB topology](../operations/deployment#foundationdb-topology) and [FoundationDB operations](../operations/foundationdb).

## SQLite

SQLite is a complete single-node implementation, not a development-only fallback. It includes:

- event log tables
- index metadata
- admin state
- projector checkpoints
- JSON criteria queries
- the same CCC save semantics as PostgreSQL

SQLite creates one event-log database file and one metadata database file per boundary in `ORISUN_SQLITE_DIR`. `{boundary}_metadata.db` stores projector checkpoints, admin users, count caches for that boundary. Keeping derived operational state out of the boundary event files prevents projector writes from contending with the event writer.

```bash
ORISUN_BACKEND=sqlite
ORISUN_SQLITE_DIR=/var/lib/orisun/sqlite
ORISUN_NATS_CLUSTER_ENABLED=false
```

Use `ORISUN_SQLITE_SYNCHRONOUS=FULL` for production durability. `FULL` is the
recommended setting because an acknowledged SQLite WAL commit is fsynced before
success returns. `NORMAL` is a throughput-oriented opt-out: SQLite can defer the
fsync until checkpointing, so an OS crash or power loss can lose commits that
callers already saw as successful. Choose `NORMAL` only when that durability
window is acceptable for the deployment.

SQLite is rejected at startup when NATS clustering is enabled. There must be exactly one active Orisun writer node.

Choose SQLite when a single active node is acceptable and simplicity matters. It is a production single-node backend, not a reduced local-development mode. For throughput, durability, and failover options such as boundary sharding, Litestream, and LiteFS, see [Scaling SQLite](../operations/deployment#scaling-sqlite).

## Boundary State

A boundary is a logical domain. Boundaries isolate event logs, indexes, notification subjects, and projector checkpoints. The admin boundary contains the event-sourced boundary catalog. Use the Admin `CreateBoundary` RPC for both new and existing physical storage. Active servers and embedded stores provision and begin publishing the boundary without a restart.

PostgreSQL maps boundaries to schemas. `ORISUN_PG_ADMIN_SCHEMA` identifies the
admin boundary's schema:

```bash
ORISUN_PG_ADMIN_SCHEMA=admin
```

Application boundary placements are durable catalog state. New PostgreSQL
boundaries specify their schema in the command placement and do not need an
environment mapping. Existing storage must use the current format. SQLite maps
each catalogued boundary to files:

```text
/var/lib/orisun/sqlite/orders.db
/var/lib/orisun/sqlite/orders_metadata.db
/var/lib/orisun/sqlite/orisun_admin.db
/var/lib/orisun/sqlite/orisun_admin_metadata.db
```

FoundationDB maps each boundary to tuple-encoded key ranges under
`ORISUN_FDB_ROOT`. A FoundationDB placement uses backend `foundationdb` and the
configured root as its namespace. Because this backend is beta, startup does
not discover or migrate legacy key ranges into the catalog; define boundaries
through `CreateBoundary`.

## Migrating between backends

The public API is the same across supported backends, but storage files,
keyspaces, and database schemas are backend-specific. Treat a backend change as
an event replay:

1. Stop writes to the source or establish an application-level cutover point.
2. On the target deployment, call `CreateBoundary` for each new empty physical
   boundary and wait for `ACTIVE`. Use target-backend placement values.
3. Read source events in ascending position order and replay them with
   `SaveEventsV2`, preserving event IDs and payloads. Chunk the replay within the
   target backend's transaction limits.
4. Validate event counts, representative criteria queries, indexes, and
   projectors.
5. Start consumers from target positions and move traffic. Source positions
   are not portable because the target assigns new positions.

`CreateBoundary` validates and initializes the recorded placement. It does not
copy events between backends or convert older storage formats.

Do not replay the source admin boundary as application data. Create the target
application boundaries through its own Admin service so its catalog
records placements for the target backend.
