# Orisun Feature Backlog

Orisun is not short on storage-engine work. It is short on the control plane,
consumer model, security, and developer ecosystem that turn a strong engine
into a platform.

This backlog is prioritized by production risk removed, customer leverage, and
how much reusable implementation already exists in the repository.

## P0 — Production Credibility

### 1. Boundary-scoped authorization

Add permissions such as `READ`, `APPEND`, `SUBSCRIBE`, `MANAGE_INDEXES`, and
`MANAGE_BOUNDARY`, scoped per boundary.

Currently every authenticated user can read every boundary and invoke every
Admin RPC, including `CreateBoundary`. See
[Security & Authorization](docs/docs/operations/security.md#permission-matrix).

### 2. Real identity and session management

Add:

- expiring and revocable sessions,
- logout,
- API keys and service accounts,
- credential rotation,
- OIDC/JWT support,
- optional LDAP or external identity-provider integration.

Current tokens are process-local map entries with no visible expiry or
revocation mechanism. See `admin/auth.go`.

### 3. Server-side idempotent writes

Make retries safe with an `idempotency_key` or a unique
`(boundary, event_id)` contract:

- same ID and same payload returns the original position,
- same ID and different payload is rejected,
- deduplication and the CCC check happen in the same transaction.

Orisun currently does not deduplicate writes by `event_id`. See the
[EventStore API](docs/docs/api/eventstore.md#data-model).

### 4. Durable subscription groups

Introduce a proper subscription service with:

- server-persisted configuration and checkpoints,
- explicit `ACK` and `NACK`,
- retry and delayed retry,
- maximum delivery count,
- parked or dead-letter events and replay,
- configurable in-flight limits,
- single-consumer ordered and competing-consumer modes,
- partition or pinning keys for preserving order within a business scope.

Today `subscriber_name` primarily obtains a lock, while the client supplies and
persists its own position. The live consumer is ephemeral and uses
`AckNonePolicy`. See `orisun/eventstore.go`.

Comparable operational models:

- [KurrentDB persistent subscriptions](https://docs.kurrent.io/server/v25.1/features/persistent-subscriptions)
- [NATS JetStream consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)

### 5. OpenTelemetry metrics

Orisun now exports gRPC request counts, active calls, duration histograms, and
status codes over OTLP. Prometheus integration belongs in the OpenTelemetry
Collector rather than an application-local scrape endpoint.

Still add first-class metrics for:

- commits, events, bytes, and latency,
- CCC conflicts by boundary and criterion shape,
- publisher head, checkpoint, and lag,
- subscriber lag, reconnects, retries, and parked events,
- boundary lifecycle failures,
- index build progress and query latency,
- SQLite group-commit queue depth,
- NATS memory and retention pressure,
- PostgreSQL pools,
- FoundationDB transaction retries.

See
[Observability](docs/docs/operations/observability.md).

Use standard RPC, database, and messaging conventions where possible:
[OpenTelemetry metrics conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/).

### 6. Health, readiness, and server information APIs

Standard unauthenticated gRPC health now reports startup readiness for the
server, EventStore, and Admin services. Authenticated `GetServerInfo` reports
version, commit, build time, backend, runtime node ID, and typed capabilities.
Readiness continuously probes JetStream and durable admin storage, transitions
to `NOT_SERVING` on dependency failure, and recovers automatically.
Extend this with:

- readiness based on individual active-boundary health,
- cluster and node health,
- current publisher ownership.

`Ping` still returns an empty response, and health does not yet expose
publisher or per-boundary diagnostics.

### 7. Complete index lifecycle management

`ListIndexes` and `GetIndex` now expose Orisun-managed definitions and their
`BUILDING` or `READY` state across PostgreSQL, SQLite, and FoundationDB.

Still add:

- `CreateIndexAsync`,
- `RebuildIndex`,
- `ExplainCriteria`,
- index states such as `BUILDING`, `READY`, `FAILED`, and `DROPPING`,
- rows-scanned and backfill-position progress.

FoundationDB operators can now inspect readiness through the public API. See
[FoundationDB operations](docs/docs/operations/foundationdb.md#indexes-and-query-shape).

`ExplainCriteria` should report the selected index, scan risk, and missing-index
recommendations.

### 8. Rate limits, quotas, and payload policy

Provide per-principal and per-boundary limits for:

- request rate,
- concurrent subscriptions,
- events per append,
- payload bytes,
- query cost,
- index operations.

This protects the shared gRPC, database, and in-memory JetStream surfaces.

## P1 — Operability and Adoption

### 9. Real administration console and CLI

Build an operator-facing surface for:

- event browsing and criteria-query construction,
- boundary lifecycle and failure retry,
- users, roles, sessions, and API keys,
- indexes and build progress,
- subscription lag and parked events,
- publisher ownership and lag,
- server configuration and diagnostics.

The repository already contains an incomplete dashboard skeleton, an
admin-port configuration, and a stale `new-admin-datastar` branch.

### 10. Exact-copy backend migration

Revive the `migrator` branch. Commit `31faf70` already contains an offline
PostgreSQL-to-SQLite and SQLite-to-PostgreSQL migration tool that preserves:

- event positions,
- timestamps,
- publisher and projector checkpoints,
- admin users,
- read models,
- index metadata.

It needs updating for the authoritative boundary catalog and FoundationDB.
This is preferable to replay-based migration, which regenerates positions.
See [Deployment](docs/docs/operations/deployment.md#graduate-to-postgresql).

### 11. Boundary lifecycle operations

Add:

- deactivate and drain,
- immediate retry for failed provisioning,
- repair or replacement of a failed definition,
- ownership transfer,
- backend migration,
- rename aliases,
- guarded deletion and physical cleanup.

Boundaries currently have immutable definitions with no rename, placement
update, or delete RPC. See
[Admin API](docs/docs/api/admin.md#boundary-lifecycle).

### 12. Backup and restore orchestration

Add backend-aware commands and status APIs for:

- consistent SQLite online backups,
- PostgreSQL backup manifests and restore validation,
- FoundationDB backup-agent integration,
- catalog, event-log, checkpoint, and index verification,
- scheduled restore drills,
- point-in-time recovery tooling.

### 13. Schema registry and compatibility checks

Register JSON Schema, Protobuf, or Avro contracts per event type and version.
Validate writes and enforce backward, forward, full, or transitive
compatibility.

Orisun already documents application-side schema evolution. Server governance
would prevent invalid events from becoming permanent history.

Reference:
[Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

### 14. Richer criteria language

Create a typed Query V2 supporting:

- `=`, `!=`, `>`, `>=`, `<`, and `<=`,
- `IN`,
- existence and null checks,
- array containment,
- nested paths,
- typed numeric, boolean, timestamp, and text values,
- metadata queries,
- explicit AND/OR trees.

The present API exposes string-valued equality tags. The exact same predicate
semantics must drive reads, CCC checks, subscriptions, and indexes on every
backend.

### 15. Managed projector toolkit

Avoid arbitrary business code inside the database. Provide a projection
runner or sidecar SDK with:

- registration and status,
- transactional checkpoint helpers,
- an idempotency inbox,
- pause, resume, reset, and rebuild,
- shadow rebuild and atomic cutover,
- retries and a dead-letter queue,
- lag metrics.

### 16. Connectors

Provide durable, checkpointed sinks for:

- Kafka,
- HTTP and webhooks,
- NATS,
- S3-compatible object storage,
- PostgreSQL,
- OpenSearch,
- ClickHouse.

Include retry, dead-letter handling, replay, filtering, transformation, and
delivery status.

### 17. Testing kit and in-memory backend

Revive the stale `next` branch's in-memory implementation for deterministic
unit tests. Package it with:

- Testcontainers helpers,
- fixture import and export,
- fake clocks and deterministic positions,
- CCC race-testing utilities,
- consumer deduplication and replay test harnesses.

### 18. Finish package distribution

Publish:

- Node.js to npm,
- Java to Maven Central,
- new official Python, .NET, and Rust clients.

The Node package is still GitHub-only and Java currently requires GitHub
Packages credentials. See [Clients](docs/docs/api/clients.md#install).

### 19. SDK resilience

Standardize across every client:

- safe retry policies,
- CCC conflict types,
- idempotency helpers,
- authentication refresh,
- DNS-based load balancing,
- tracing,
- deadlines,
- subscription reconnection,
- checkpoint helpers.

### 20. Documentation and release synchronization

The changelog is at `0.9.1`, while the overview still says the documentation
targets `0.6.1`.

Automate:

- documentation version injection,
- generated API reference,
- client compatibility matrices,
- broken-link checks,
- package-publication verification.

## P2 — Differentiation and Scale

### 21. Flutter and embedded mobile Orisun

The `mobile-experience` branch is the largest sleeping asset. It contains a
NATS-free embedded SQLite runtime, Flutter FFI, Android, iOS, and desktop
packaging, tests, and documentation.

This could make Orisun unusually compelling for offline-first applications.

### 22. Boundary routing and rebalancing

Add service discovery and a routing API so clients can locate the node owning a
boundary. Follow with:

- drain-and-transfer workflows,
- routing-table refresh,
- placement health,
- controlled rebalancing.

### 23. Kubernetes operator and Helm chart

Automate:

- topology validation,
- rolling upgrades,
- TLS,
- backups,
- NATS clustering,
- one-node-first migrations,
- readiness gates,
- per-backend configuration.

### 24. Bulk ingestion and streaming appends

Add a client-streaming append and import API with:

- bounded batching,
- resumable checkpoints,
- validation-only mode,
- backpressure,
- progress reporting.

Reuse lessons from the two dormant batching experiment branches without
reviving their old architectures wholesale.

### 25. Encryption and key management

Add:

- envelope encryption,
- external KMS or Vault integration,
- per-boundary keys,
- rotation,
- encrypted metadata options,
- auditable key access.

### 26. Privacy and controlled redaction

Support:

- tombstone workflows,
- crypto-shredding,
- a highly privileged and audited redaction tool.

Append-only history eventually collides with PII deletion requirements.
Reference:
[KurrentDB redaction](https://docs.kurrent.io/server/v25.1/operations/redaction).

### 27. Tamper-evident boundaries

Hash-chain committed batches or maintain Merkle checkpoints, optionally signed
externally.

This would let Orisun prove that decision history has not been modified
directly in PostgreSQL, SQLite, or FoundationDB. It strongly complements
Orisun's positioning around decisions that must stay correct.

### 28. Cold archive and tiered storage

Move older payloads to S3-compatible storage while retaining positions,
indexes, and transparent reads. Add retention policies without making the live
broker the source of truth.

### 29. Read-only analytics interface

Offer:

- Parquet or Arrow export,
- a read-only SQL or Flight SQL endpoint,
- integration with analytics replicas.

Keep analytics isolated from CCC transaction paths.

### 30. FoundationDB graduation

Before declaring the FoundationDB backend generally available, add:

- chaos and multi-node soak suites,
- index recovery and status APIs,
- backup verification,
- upgrade compatibility guarantees,
- transaction-budget metrics,
- explicit support guarantees.

## Sleeping Code Worth Recovering

| Asset | Recommendation |
| --- | --- |
| `mobile-experience` | Recover; it is substantial and relatively recent. |
| `migrator` | Recover and port to the 0.9 catalog model. |
| `next` in-memory backend | Recover as a testing package, not a production backend. |
| `sqlite-litefs-replication` | Mine for backup and failover work; do not advertise multi-writer SQLite. |
| `new-admin-datastar` | Treat as a design seed only; it is stale and small. |
| WAL publisher experiment | Do not revive without re-proving stable-prefix, no-skip, and ordering guarantees. |
| Batch experiments | Extract benchmarks and batching lessons; the current architecture has moved on. |

## Suggested Sequence

### 0.10 — Trust

- boundary-scoped authorization,
- session hardening,
- idempotent writes,
- metrics and health,
- index lifecycle.

### 0.11 — Operate

- durable subscriptions,
- Admin console and CLI,
- exact-copy migrator,
- boundary lifecycle,
- backup and restore tooling.

### 0.12 — Build

- schema registry,
- Query V2,
- projector toolkit,
- connectors,
- testing kit,
- package distribution.

### 1.0 — Production Contract

- external security audit,
- upgrade guarantees,
- FoundationDB graduation criteria,
- Kubernetes deployment story,
- disaster-recovery certification.

### Parallel Product Bet

- mobile and Flutter embedded Orisun.

## What Not to Prioritize

Avoid spending the next cycle on:

- another storage backend,
- aggregate or stream APIs that dilute CCC,
- active-active SQLite,
- cross-boundary transactions,
- “exactly once” side-effect marketing,
- reviving the old WAL publisher without re-proving ordering and no-miss
  guarantees.

CCC, content queries, and boundary ordering are Orisun's moat. The missing
layer is everything that makes customers comfortable betting production
systems on it.
