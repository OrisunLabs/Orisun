---
title: Deployment
description: Run Orisun as SQLite, PostgreSQL, or a PostgreSQL-backed cluster.
---

Orisun can run as a standalone release binary, a Docker image, or an embedded Go package. The same `ORISUN_` environment variables configure each mode; choose the packaging model that fits your platform.

## Binary deployments

Use the release binary when you want Orisun supervised like any other server process. This is a good fit for systemd, Nomad, Kubernetes containers built from your own base image, VM deployments, and PaaS platforms that run a command directly.

Release assets are backend-specific:

| Binary | Use when |
| --- | --- |
| `orisun-pg-<os>-<arch>` | The deployment only uses PostgreSQL. |
| `orisun-sqlite-<os>-<arch>` | The deployment only uses SQLite. |

For direct binary deployment:

- run the process under a supervisor that restarts it on failure
- persist `ORISUN_NATS_STORE_DIR`
- persist `ORISUN_SQLITE_DIR` when using SQLite
- inject secrets through the platform's secret manager
- expose `ORISUN_GRPC_PORT` only to trusted clients unless TLS and auth policy are configured

Example systemd unit:

```ini
[Unit]
Description=Orisun event store
After=network-online.target
Wants=network-online.target

[Service]
User=orisun
Group=orisun
EnvironmentFile=/etc/orisun/orisun.env
ExecStart=/usr/local/bin/orisun-sqlite
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

The environment file contains the same settings shown in [Getting Started](../getting-started).

## Container deployments

Use the published Docker images when your platform already standardizes on containers, or when you want the simplest way to run the official release artifact.

Images are published to both Docker Hub and GitHub Container Registry. The Docker Hub tags mirror the binary flavors:

| Image | Backend |
| --- | --- |
| `orisunlabs/orisun:pg` | PostgreSQL |
| `orisunlabs/orisun:sqlite` | SQLite only |

The same tags are also available under `ghcr.io/orisunlabs/orisun`.

Persist the same directories you would persist for a binary deployment. Containers do not change the storage model.

## Standalone SQLite

SQLite is the simplest production-capable single-node setup. It does not need a separate database container.

Use this profile when:

- one active Orisun node is enough
- deployment simplicity matters
- the event store should be embedded or edge-friendly

SQLite must run with `ORISUN_NATS_CLUSTER_ENABLED=false`.

Operational notes:

- Persist `/var/lib/orisun` or the configured `ORISUN_SQLITE_DIR`.
- Run exactly one active Orisun writer node.
- Back up every `{boundary}.db` file, every `{boundary}_metadata.db` file, and the NATS store directory if live delivery retention matters during restore.
- Treat the admin boundary files as mandatory: its event log contains the
  boundary catalog. Restoring application files without the matching admin
  boundary requires an explicit `CreateBoundary` call before those files are usable.

## Scaling SQLite

SQLite has no clustered mode, but a single boundary file goes further than most workloads need, and Orisun's boundary model gives you a sharding path before you have to change backends. Scale in this order.

### 1. Vertical headroom first

Each boundary file already runs WAL mode with a read pool sized to `runtime.NumCPU()` and a single serialized writer. On NVMe storage with batched `SaveEventsV2` calls, a single boundary sustains tens of thousands of events per second. Before adding infrastructure:

- batch writes, because the per-transaction cost dominates the per-event cost
- keep `ORISUN_SQLITE_DIR` on local NVMe, never on NFS or other network filesystems (file locking is unreliable there)
- raise `LimitNOFILE` and give the node enough memory for the page cache

The per-boundary write ceiling is fundamental: one writer per file, and subscription reads require stable position order per boundary. This same per-boundary ordering ceiling exists in PostgreSQL mode; SQLite just reaches it sooner.

### 2. Shard by boundary

A boundary is a complete, independent event-log unit: its own database file, metadata file, and position counter. Orisun has no cross-boundary transactions, so boundaries shard cleanly across nodes with no coordination:

- run N independent single-node Orisun deployments
- place a disjoint set of `{boundary}.db` files in each node's `ORISUN_SQLITE_DIR`
- route client requests by boundary to the owning node (client-side routing table, or a gRPC proxy that switches on the boundary field)

Each node remains a normal standalone SQLite deployment. There is no shared storage, consensus, or rebalancing protocol. Moving a boundary to another node is a maintenance operation:

1. Stop the source node and run a final WAL checkpoint.
2. Copy `{boundary}.db` and `{boundary}_metadata.db` into the target node's
   `ORISUN_SQLITE_DIR`.
3. Start the target and call `CreateBoundary` with backend `sqlite` and a namespace equal to
   the boundary name.
4. Wait for `GetBoundary` to report `ACTIVE`, then update routing.

The source catalog still contains its old immutable definition; do not restart
the source against its old copy after traffic moves. Orisun does not currently
provide a delete or ownership-transfer RPC.

If one boundary alone outgrows a node, sharding cannot help. Split the domain into more boundaries or move that deployment to PostgreSQL.

### 3. Durability and failover

The gap in a single-node deployment is availability, not throughput. Two complementary tools:

**[Litestream](https://litestream.io)** continuously replicates SQLite WAL segments to S3-compatible storage. It runs as a sidecar, needs no Orisun changes, and gives a recovery point of seconds. Replicate every `{boundary}.db` and `{boundary}_metadata.db` in `ORISUN_SQLITE_DIR`. Recovery is a restore-and-restart: minutes of downtime, near-zero data loss. This should be the baseline for any production SQLite deployment.

**[LiteFS](https://fly.io/docs/litefs/)** replicates the files to warm standby machines with lease-based primary election, cutting failover from minutes to seconds. Run Orisun only on the current primary: Orisun is not read-only-aware (event and projector writes require an active writer), so a second Orisun process must not run against a replica copy. Standbys hold warm files; on failover, the new primary starts Orisun. LiteFS adds operational moving parts (FUSE, a lease backend), so adopt it only when restore-time recovery is too slow.

Do not copy live database files with `cp` or filesystem snapshots alone; under WAL a bare file copy can be torn. Use Litestream, the SQLite backup API, or stop the node first.

What does not work: multi-writer SQLite replication (cr-sqlite, marmot, and similar eventually-consistent or CRDT systems). Orisun's consistency check must be serializable with the insert in one transaction on one writer; concurrent writers on different replicas would each pass their local check and merge conflicting histories. Only single-writer topologies preserve Orisun's guarantees.

### 4. Graduate to PostgreSQL

When a deployment needs multi-node availability or write scale beyond boundary sharding, move to the PostgreSQL backend rather than building a distributed SQLite. The public API is identical; see [Migrating between backends](../concepts/storage-backends#migrating-between-backends). Create and activate each empty target boundary before replaying its events in order with unconditional `SaveEventsV2` calls. Positions are regenerated on write, so consumers must restart subscriptions from the new positions.

### Analytics on the side

SQLite boundary files are readable by [DuckDB's sqlite extension](https://duckdb.org/docs/extensions/sqlite), so a restored backup or standby copy doubles as a zero-ETL analytics source. Point DuckDB at a copy of `{boundary}.db` and run columnar SQL over the event log without touching the write path. Always use a copy or a Litestream restore, never the live file.

## Standalone PostgreSQL

Use one Orisun node with PostgreSQL when you want the event log in PostgreSQL but do not need Orisun clustering.

This profile is useful when:

- PostgreSQL is already part of the platform
- the database needs independent backup and operational controls
- you may later add more Orisun nodes

Persist the NATS store directory for durable JetStream state. PostgreSQL remains the event source of truth.

## PostgreSQL Major Upgrades

For Orisun's event-envelope storage upgrade, use the separate
[event-envelope upgrade guide](./upgrading-event-envelope). The procedure below
addresses PostgreSQL server upgrades.

Current Orisun releases store public `commit_position` values as logical
event-store positions. PostgreSQL's internal transaction ID is retained only as
disposable `pg_xact_id` visibility metadata.

Recommended upgrade sequence:

1. Stop all Orisun nodes cleanly.
2. Back up PostgreSQL and the NATS store directory.
3. If the Orisun installation is older than `0.8.0`, upgrade to `0.8.0` and
   verify it before upgrading PostgreSQL or moving to a newer Orisun release.
4. Upgrade PostgreSQL using your platform's normal process.
5. Start one Orisun node first and wait for every catalogued boundary to
   initialize.
6. Confirm publishers/projectors are healthy, then start the rest of the Orisun nodes.

Current releases clear stale `pg_xact_id` values when a restored database or
new cluster has restarted its transaction-ID range. PostgreSQL transaction IDs
therefore do not need to be preserved for Orisun correctness. The immediately preceding storage version upgrades automatically without rewriting
event documents, positions, or projector checkpoints. Older formats are rejected.

## Clustered PostgreSQL

Clustered mode uses PostgreSQL, embedded NATS clustering, and one active notification relay per boundary.

Each node should share:

- PostgreSQL database and schemas
- `ORISUN_PG_ADMIN_SCHEMA`
- `ORISUN_NATS_CLUSTER_NAME`
- NATS cluster credentials

Each node should have unique:

- `ORISUN_GRPC_PORT`
- `ORISUN_NATS_PORT`
- `ORISUN_NATS_CLUSTER_HOST`
- `ORISUN_NATS_SERVER_NAME`
- `ORISUN_NATS_STORE_DIR`

Core NATS routes transient hints across the cluster without notification replicas. JetStream remains enabled for leases and admin messaging; use at least three nodes for a production cluster.

Expected notification relay behavior:

- One node holds the boundary relay lease and forwards backend signals as hints.
- Other nodes may log lock contention for that boundary.
- If the owner exits, another node acquires the lease and emits an initial hint. Subscription idle watchdogs publish hints if no notifications arrive; healthy NATS is required.

## PgBouncer

Session mode works out of the box.

For transaction mode:

- SQL functions use schema-qualified table references.
- The Go-side pool uses multi-statement transactions normally.
- PgBouncer 1.21+ should be configured with compatible prepared-statement handling.
- Older PgBouncer deployments should use simple protocol mode or compatible describe-cache settings.

## Runtime Tuning

| Variable | Recommendation |
| --- | --- |
| `GOMAXPROCS` | Auto-set from cgroup CPU quota through `automaxprocs`. |
| `GOMEMLIMIT` | Set to about 80 percent of container memory. |
| `GOGC` | Tune upward for lower GC frequency if memory allows. |

Effective values are logged at startup.

## Limits and sizing

| Limit | Value | Notes |
| --- | --- | --- |
| gRPC request message size | `ORISUN_GRPC_MAX_RECEIVE_MESSAGE_SIZE` (default 64 MB) | Caps one gRPC request, including `SaveEventsV2`; it therefore bounds event payload plus consistency observations. Split very large batches. |
| Event `data` / `metadata` | JSON string per field | No separate field cap; the whole request must fit the message-size limit above. |
| Subscription read batch | 100 events | Inclusive forward reads discard the cursor event; memory remains bounded per subscription. |
| `GetEvents` page | `count` per request, server-capped at 10000 | Page with `from_position`; see [Positions and Ordering](../concepts/positions#positions-and-paging). |
| Notification buffering (per subscription) | NATS client defaults; one pending drain wake-up | Core NATS retains no history. Recovery reads the backend. |
| Subscription idle watchdog | `ORISUN_SUBSCRIPTION_IDLE_THRESHOLD` (default 1s) | Positive silence threshold since the last received hint. At expiry publish a NATS hint, which triggers a backend drain. |

Sizing guidance:

- Keep batches comfortably under the configured gRPC receive limit. For bulk imports, chunk into many ordered, unconditional `SaveEventsV2` calls.
- Reuse one official client/channel per target for hot writes. The Node and Java clients set Orisun's high-throughput gRPC defaults and cache auth tokens after the first authenticated response.
- For bursty writers, cap concurrent `SaveEventsV2` calls on that one client around 512-1024 in flight. Launching every pending write at once adds client-side HTTP/2 stream and scheduler overhead without improving the single-boundary write ceiling.
- Measure backend read load as subscription counts grow. Notification retention does not determine recovery or subscriber lag.
- Subscribers that routinely fall out of the live window are served from durable storage; this is correct but increases read load. Scale retention or subscriber throughput accordingly.

## Security Checklist

- Change `ORISUN_ADMIN_PASSWORD` before production use.
- Enable gRPC TLS in production-facing deployments.
- Protect PostgreSQL credentials and NATS cluster credentials.
- Use network policy or firewall rules for PostgreSQL, gRPC, and NATS cluster routes.

## Notification transport

Deploy one server version across a cluster. Stop older subscriptions and publishers
before starting this runtime. Core NATS carries transient boundary hints; subscriptions
read ordered events from durable storage and retain their own cursors. There is no
notification JetStream event stream, publisher checkpoint, or backup polling loop.

Fresh storage initializes directly. The immediately preceding storage version
upgrades automatically; older formats are rejected. Follow the
[storage upgrade policy](./upgrading-event-envelope) for the supported source versions
and the export/import procedure for older deployments. A current-format backup preserves positions and write contexts;
reconnect subscriptions using those retained positions after restoring the catalog.
