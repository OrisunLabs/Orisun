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
| `orisun-fdb-linux-<arch>` | The deployment uses FoundationDB; beta, Linux only, requires native FDB client libraries. |

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
| `orisunlabs/orisun:pg` | PostgreSQL-compatible backends: PostgreSQL and YugabyteDB |
| `orisunlabs/orisun:sqlite` | SQLite only |
| `orisunlabs/orisun:fdb` | FoundationDB only, beta, includes the FDB client library |

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
  boundary requires an explicit `CreateBoundary` call with
  `existed_before_catalog` before those files are usable.

## Scaling SQLite

SQLite has no clustered mode, but a single boundary file goes further than most workloads need, and Orisun's boundary model gives you a sharding path before you have to change backends. Scale in this order.

### 1. Vertical headroom first

Each boundary file already runs WAL mode with a read pool sized to `runtime.NumCPU()` and a single serialized writer. On NVMe storage with batched `SaveEvents`, a single boundary sustains tens of thousands of events per second. Before adding infrastructure:

- batch writes, because the per-transaction cost dominates the per-event cost
- keep `ORISUN_SQLITE_DIR` on local NVMe, never on NFS or other network filesystems (file locking is unreliable there)
- raise `LimitNOFILE` and give the node enough memory for the page cache

The per-boundary write ceiling is fundamental: one writer per file, and the publisher requires total order per boundary. This same per-boundary ordering ceiling exists in PostgreSQL mode; SQLite just reaches it sooner.

### 2. Shard by boundary

A boundary is a complete, independent event-log unit: its own database file, metadata file, and position counter. Orisun has no cross-boundary transactions, so boundaries shard cleanly across nodes with no coordination:

- run N independent single-node Orisun deployments
- place a disjoint set of `{boundary}.db` files in each node's `ORISUN_SQLITE_DIR`
- route client requests by boundary to the owning node (client-side routing table, or a gRPC proxy that switches on the boundary field)

Each node remains a normal standalone SQLite deployment. There is no shared storage, consensus, or rebalancing protocol. Moving a boundary to another node is a maintenance operation:

1. Stop the source node and run a final WAL checkpoint.
2. Copy `{boundary}.db` and `{boundary}_metadata.db` into the target node's
   `ORISUN_SQLITE_DIR`.
3. Start the target and call `CreateBoundary` with
   `existed_before_catalog: true`, backend `sqlite`, and a namespace equal to
   the boundary name.
4. Wait for `GetBoundary` to report `ACTIVE`, then update routing.

The source catalog still contains its old immutable definition; do not restart
the source against its old copy after traffic moves. Orisun does not currently
provide a delete or ownership-transfer RPC.

If one boundary alone outgrows a node, sharding cannot help. Split the domain into more boundaries or move that deployment to PostgreSQL.

### 3. Durability and failover

The gap in a single-node deployment is availability, not throughput. Two complementary tools:

**[Litestream](https://litestream.io)** continuously replicates SQLite WAL segments to S3-compatible storage. It runs as a sidecar, needs no Orisun changes, and gives a recovery point of seconds. Replicate every `{boundary}.db` and `{boundary}_metadata.db` in `ORISUN_SQLITE_DIR`. Recovery is a restore-and-restart: minutes of downtime, near-zero data loss. This should be the baseline for any production SQLite deployment.

**[LiteFS](https://fly.io/docs/litefs/)** replicates the files to warm standby machines with lease-based primary election, cutting failover from minutes to seconds. Run Orisun only on the current primary: Orisun is not read-only-aware (publishers and checkpoints write continuously), so a second Orisun process must not run against a replica copy. Standbys hold warm files; on failover, the new primary starts Orisun. LiteFS adds operational moving parts (FUSE, a lease backend), so adopt it only when restore-time recovery is too slow.

Do not copy live database files with `cp` or filesystem snapshots alone; under WAL a bare file copy can be torn. Use Litestream, the SQLite backup API, or stop the node first.

What does not work: multi-writer SQLite replication (cr-sqlite, marmot, and similar eventually-consistent or CRDT systems). Orisun's consistency check must be serializable with the insert in one transaction on one writer; concurrent writers on different replicas would each pass their local check and merge conflicting histories. Only single-writer topologies preserve Orisun's guarantees.

### 4. Graduate to PostgreSQL

When a deployment needs multi-node availability or write scale beyond boundary sharding, move to the PostgreSQL backend rather than building a distributed SQLite. The public API is identical; see [Migrating between backends](../concepts/storage-backends#migrating-between-backends). Create and activate each empty target boundary before replaying its events in order with `SaveEvents`. Positions are regenerated on write, so consumers must restart subscriptions from the new positions.

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
therefore do not need to be preserved for Orisun correctness. The older event
shape, logical-position, and checkpoint migrations are intentionally confined
to the required `0.8.0` bridge release.

## Clustered PostgreSQL

Clustered mode uses PostgreSQL, embedded NATS clustering, and one active publisher per boundary.

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

Minimum recommendation: three nodes for JetStream quorum.

Expected publisher behavior:

- The node that owns a boundary logs successful lock acquisition.
- Other nodes may log lock contention for that boundary.
- If the owner exits, another node resumes from the PostgreSQL checkpoint.

## YugabyteDB

YugabyteDB is deployed through the PostgreSQL-compatible backend:

```bash
ORISUN_BACKEND=postgres
ORISUN_PG_DIALECT=yugabyte
ORISUN_PG_PORT=5433
ORISUN_PG_LISTEN_ENABLED=true
```

Use YugabyteDB `v2025.2.3` or later. Enable `LISTEN/NOTIFY` on both Masters and TServers:

```bash
--master_flags=ysql_yb_enable_listen_notify=true
--tserver_flags=ysql_yb_enable_listen_notify=true
```

Orisun requires YugabyteDB advisory locks and `LISTEN/NOTIFY`. Advisory locks are enabled by default in YugabyteDB `v2025.1+`. `LISTEN/NOTIFY` is disabled by default in YugabyteDB and must be enabled before Orisun starts. If `pg_notify` is unavailable, saves fail.

YugabyteDB does not provide PostgreSQL internal transaction ID semantics, so Orisun uses a per-boundary committed-position watermark for stable-prefix reads. This is selected automatically by `ORISUN_PG_DIALECT=yugabyte`; do not run YugabyteDB with the default `postgres` dialect.

For clustered Orisun nodes, use the same guidance as [Clustered PostgreSQL](#clustered-postgresql): all Orisun nodes share the same YugabyteDB database, boundaries, schema mapping, and NATS cluster configuration.

## FoundationDB topology

FoundationDB support in Orisun is beta. Use this topology for controlled production pilots, but expect FDB-specific storage layout, index internals, and operational defaults to remain eligible for breaking changes until the backend graduates from beta.

A FoundationDB deployment has three independently scalable tiers:

```text
                gRPC clients
                     │
       ┌─────────────┼─────────────┐
       │ Orisun node │ Orisun node │ Orisun node     stateless tier
       │  + NATS     │  + NATS     │  + NATS         JetStream cluster (quorum: 3)
       └──────┬──────┴──────┬──────┴──────┬───┘
              │  libfdb_c + fdb.cluster file
       ┌──────┴─────────────┴─────────────┴───┐
       │           FoundationDB cluster        │
       │  coordinators (3) · proxies/resolvers │
       │  transaction logs (NVMe) · storage    │
       └───────────────────────────────────────┘
```

**Orisun tier.** Orisun nodes are effectively stateless with this backend: all durable state lives in FoundationDB. NATS JetStream is the live-delivery buffer; publisher ownership, checkpoints, indexes, users, and projector state live in FoundationDB. Scale horizontally behind any gRPC load balancer. One publisher per boundary self-elects through an FDB lease lock and fails over automatically. Run at least three Orisun nodes when NATS clustering is enabled, with the same shared/unique variable split as [Clustered PostgreSQL](#clustered-postgresql) (minus the PostgreSQL variables, plus `ORISUN_FDB_CLUSTER_FILE`).

**FoundationDB tier by size:**

| Tier | Layout | Redundancy |
| --- | --- | --- |
| Starter | 3 machines, each running storage + log + stateless `fdbserver` processes | `double ssd` |
| Production | 3 dedicated log-class machines on NVMe + 5 or more storage-class + 2–3 stateless-class (proxies, resolvers) | `triple ssd` |
| Multi-region | Primary + synchronous satellite for transaction logs + asynchronous remote region | `triple` with satellite redundancy |

Process classes are the scaling levers:

- **Transaction logs** sit on the commit critical path because every write waits for a tLog fsync. Give them dedicated NVMe and nothing else to do.
- **Storage servers** absorb reads and background data movement; add them for data volume and read throughput.
- **Proxies and resolvers** are stateless CPU-bound processes; add them when commit throughput saturates.

Orisun's write scaling rides this directly: positions come from commit versionstamps, so adding proxies, logs, and storage raises parallel commit throughput while every boundary stays totally ordered.

**Kubernetes.** Use the official [fdb-kubernetes-operator](https://github.com/FoundationDB/fdb-kubernetes-operator) with a `FoundationDBCluster` resource. The operator maintains the cluster file in a ConfigMap; mount it into Orisun pods and point `ORISUN_FDB_CLUSTER_FILE` at it. Orisun itself is a plain Deployment.

**Operational notes:**

- The `libfdb_c` client major version must match the server. The multi-version client allows rolling server upgrades; use the published `orisunlabs/orisun:fdb` image or bake the client library into your own Orisun image. Release FDB binaries are Linux-only and still require the client library at runtime.
- Backups: `fdbbackup` agents stream continuous backups to S3-compatible storage with point-in-time restore. This replaces the PostgreSQL dump/restore story.
- Monitoring: feed `fdbcli status json` (or an exporter built on it) into your metrics stack. Watch commit latency, transaction log queue depth, storage lag, and transaction conflict rate. Conflict rate maps directly to Orisun consistency-condition retries on contended aggregates.
- Coordinators: 3 spread across failure domains in one datacenter, 5 across multiple.
- Bound client-side stalls with `ORISUN_FDB_TRANSACTION_TIMEOUT_MS` (default 10s) so a partitioned cluster surfaces as errors instead of hung requests.
- Production runbook: see [FoundationDB operations](./foundationdb) for client libraries, cluster-file handling, backups, failover expectations, and release gates.


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
| gRPC request message size | `ORISUN_GRPC_MAX_RECEIVE_MESSAGE_SIZE` (default 64 MB) | Caps a single `SaveEvents` request, so it bounds batch size times event payload. Split very large batches. |
| Event `data` / `metadata` | JSON string per field | No separate field cap; the whole request must fit the message-size limit above. |
| Publisher read batch | `ORISUN_POLLING_PUBLISHER_BATCH_SIZE` (default 1000) | Events drained per publisher read cycle. Raise for high write volume, lower to smooth memory. |
| `GetEvents` page | `count` per request, server-capped at 10000 | Page with `from_position`; see [Positions and Ordering](../concepts/positions#positions-and-paging). |
| Live retention (per boundary) | `ORISUN_NATS_EVENT_STREAM_MAX_BYTES` (512 MB), `_MAX_MSGS`, `_MAX_AGE` (5m) | In-memory live buffer only. Size for your slowest subscriber, not for durability. |

Sizing guidance:

- Keep batches comfortably under the configured gRPC receive limit. For bulk imports, chunk into many ordered `SaveEvents` calls.
- Reuse one official client/channel per target for hot writes. The Node and Java clients set Orisun's high-throughput gRPC defaults and cache auth tokens after the first authenticated response.
- For bursty writers, cap concurrent `SaveEvents` calls on that one client around 512-1024 in flight. Launching every pending write at once adds client-side HTTP/2 stream and scheduler overhead without improving the single-boundary write ceiling.
- Set retention age above the slowest subscriber's expected lag and above the catch-up handover grace (~10s).
- Subscribers that routinely fall out of the live window are served from durable storage; this is correct but increases read load. Scale retention or subscriber throughput accordingly.

## Security Checklist

- Change `ORISUN_ADMIN_PASSWORD` before production use.
- Enable gRPC TLS in production-facing deployments.
- Protect PostgreSQL credentials and NATS cluster credentials.
- Use network policy or firewall rules for PostgreSQL, gRPC, and NATS cluster routes.
