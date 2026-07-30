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

PostgreSQL is the clustered backend. Multiple Orisun nodes can share one database. Publishers coordinate with PostgreSQL advisory locks so only one active publisher owns a boundary at a time. Writes also take a short per-boundary advisory lock while drawing public positions and committing, which preserves commit-ordered positions across concurrent writers.

PostgreSQL stores:

- event log tables
- per-boundary position state
- publisher checkpoints
- projector checkpoints
- index metadata
- admin state

Choose PostgreSQL when you need horizontal Orisun nodes, database-managed backup/restore, mature operational tooling, or PgBouncer integration.

The write lock is per boundary, not global. It is intentional: Command Context Consistency relies on public positions being a stable upper bound for committed events in that boundary. Split unrelated high-write domains into separate boundaries when they do not share invariants.

### PostgreSQL position metadata

PostgreSQL mode stores two ordering-related values:

- `transaction_id`: Orisun's logical commit position, durable across PostgreSQL major upgrades and restore workflows.
- `pg_xact_id`: PostgreSQL's internal transaction ID, used only as a current-cluster visibility marker so publishers and catch-up reads do not skip older open transactions.

Do not use PostgreSQL internal transaction IDs as application cursors. The
required `0.8.0` bridge release remaps older Orisun databases that exposed
PostgreSQL transaction IDs as public commit positions; later releases no longer
carry that one-time migration. See
[Positions and Ordering](./positions#postgresql-transaction-ids) and
[Deployment](../operations/deployment#postgresql-major-upgrades).

## FoundationDB

FoundationDB is a beta clustered backend built on ordered key-value transactions. Use the `orisun-fdb` release binary, the `orisunlabs/orisun:fdb` / `ghcr.io/orisunlabs/orisun:fdb` image, or build your own binary with the `foundationdb` build tag and native FoundationDB client libraries.

FoundationDB stores:

- event records in ordered per-boundary key ranges
- publisher checkpoints
- projector checkpoints
- admin state
- index metadata and secondary index keys
- watch signal keys for publisher wake-ups
- token-fenced publisher lease locks

```bash
go build -tags foundationdb ./cmd/orisun-fdb

ORISUN_BACKEND=foundationdb
ORISUN_FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ORISUN_FDB_ROOT=orisun
```

Criteria queries keep the same public API, but FoundationDB requires ready covering boundary indexes for criteria reads and consistency checks. Unindexed criteria fail with `FAILED_PRECONDITION`; this avoids boundary-wide scans and keeps write conflict ranges scoped to the indexed event subset.

FoundationDB assigns event positions with commit versionstamps instead of a per-boundary counter. Plain appends can commit in parallel; writes with a consistency context conflict only on the covered index range for that context, so commands on different aggregates in one boundary commit concurrently. As in the other backends, an `expected_position` takes effect only together with consistency criteria.

For cluster layout, process classes, Kubernetes, backups, monitoring, lock failover, and release gates, see [FoundationDB topology](../operations/deployment#foundationdb-topology) and [FoundationDB operations](../operations/foundationdb).

## SQLite

SQLite is a complete single-node implementation, not a development-only fallback. It includes:

- event log tables
- index metadata
- admin state
- publisher checkpoints
- projector checkpoints
- JSON criteria queries
- the same CCC save semantics as PostgreSQL

SQLite creates one event-log database file and one metadata database file per boundary in `ORISUN_SQLITE_DIR`. `{boundary}_metadata.db` stores publisher checkpoints, projector checkpoints, admin users, and count caches for that boundary. Keeping derived operational state out of the boundary event files prevents publisher/projector writes from contending with the event writer.

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

A boundary is a logical domain. Boundaries isolate event logs, indexes, publisher checkpoints, and projector checkpoints. The admin boundary contains the event-sourced boundary catalog. Use the Admin `CreateBoundary` RPC for both new and existing physical storage; set `existed_before_catalog` when adopting storage that predates the catalog definition. Active servers and embedded stores provision and begin publishing the boundary without a restart.

PostgreSQL maps boundaries to schemas. `ORISUN_PG_ADMIN_SCHEMA` identifies the
admin boundary's schema:

```bash
ORISUN_PG_ADMIN_SCHEMA=admin
```

Application boundary placements are durable catalog state. New PostgreSQL
boundaries specify their schema in the command placement and do not need an
environment mapping. Existing installations from before 0.8.0 must upgrade
through 0.8.0 so its legacy `ORISUN_PG_SCHEMAS` importer can create that
catalog. SQLite maps each catalogued boundary to files:

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
   `SaveEvents`, preserving event IDs and payloads. Chunk the replay within the
   target backend's transaction limits.
4. Validate event counts, representative criteria queries, indexes, and
   projectors.
5. Start consumers from target positions and move traffic. Source positions
   are not portable because the target assigns new positions.

`CreateBoundary` is not a cross-backend data-copy operation. Setting
`existed_before_catalog` only records that compatible storage is already
present; it does not copy events.

Do not replay the source admin boundary as application data. Create the target
application boundaries through its own Admin service so its catalog
records placements for the target backend.
