# Changelog

## Unreleased

### Changed

- Admin RPCs now enforce the existing role model. Boundary and user
  administration require `ADMIN`; boundary inventory and event counts allow
  `ADMIN` or `OPERATIONS`; password changes remain authenticated self-service.
- `CreateUser` now rejects unsupported or incorrectly cased role values instead
  of storing roles that can never satisfy authorization checks.

## 0.9.1 - 2026-07-24

### Fixed

- Stabilized the YugabyteDB LISTEN/NOTIFY integration test by retrying only
  Yugabyte's transient SQLSTATE `55000` response while its notification
  internals finish initializing. Other listener errors still fail immediately.

### Changed

- Release publication now verifies successful CI for the exact tagged `main`
  commit instead of running the full test suite a second time. Manual release
  reruns also build from the resolved tag commit.
- Updated GitHub Release publishing to the Node 24-compatible
  `softprops/action-gh-release` v3 action.

### Release Notes

- This release carries forward all 0.9.0 breaking changes, behavior changes,
  and migration notes below.

## 0.9.0 - 2026-07-24

### Breaking Changes

- Replaced `ORISUN_PG_SCHEMAS` with `ORISUN_PG_ADMIN_SCHEMA`. PostgreSQL now
  bootstraps only the admin boundary; application boundary placements are
  replayed exclusively from the event-backed catalog. Existing installations
  from 0.7.x or earlier must not skip 0.8.0, which imports the complete legacy
  mapping. Startup now rejects a pre-existing PostgreSQL admin store without
  that catalog migration.
- Removed the tuple-return backend initialization functions. Go callers must
  use `InitializePostgresDatabaseRuntime`, `InitializeSqliteDatabaseRuntime`,
  `InitializeSqliteDatabaseRuntimeWithLockProvider`, or
  `InitializeFoundationDBRuntime`.
- Removed `InitialBoundaries` and the startup boundary-list parameters from
  backend and EventStore runtime APIs. Startup bootstraps only the configured
  admin boundary; application boundaries start exclusively through catalog
  replay.

### Changed

- Removed post-0.8 legacy boundary reconciliation. Fresh installations
  bootstrap only the admin boundary; all application boundaries are installed
  by replaying the durable catalog.
- Removed bridge-only storage migrations for legacy event columns, PostgreSQL
  logical positions and checkpoints, and pre-0.8 SQLite metadata layouts.
  Current schema initialization and restore-safe position handling remain.
- Removed rolling-upgrade compatibility for pre-0.8 literal JetStream lock
  values. Current nodes accept only versioned, expiring lease records.

### Migration Notes

- Existing installations must run 0.8.0 and verify that every expected
  boundary is `ACTIVE` before upgrading to this release.
- After the 0.8.0 catalog is verified, replace the remaining PostgreSQL admin
  mapping with `ORISUN_PG_ADMIN_SCHEMA=<admin-schema>`.

## 0.8.0 - 2026-07-24

### Breaking Changes

- Removed `ORISUN_BOUNDARIES`. Application boundaries are now immutable,
  event-backed definitions in the admin catalog and can be created or imported
  while the server is running.
- Embedded `SubscribeToEvents` now accepts an `eventstore.SubscribeRequest`
  and a synchronous `eventstore.EventHandler`. Embedded callers no longer
  receive generated `Event` messages through `MessageHandler`.
- Generated Go protobuf messages and gRPC stubs now live together in
  `github.com/OrisunLabs/Orisun/orisun/grpcapi`. Server-module callers that
  used generated request, response, event, or client types from
  `github.com/OrisunLabs/Orisun/orisun` must update that import. The gRPC wire
  contract is unchanged.

### Added

- Added Admin `CreateBoundary`, `ListBoundaries`, and
  `GetBoundary` RPCs, including asynchronous `PROVISIONING`, `ACTIVE`, and
  `FAILED` lifecycle states.
- Added matching boundary-management methods to the PostgreSQL, SQLite, and
  FoundationDB Go embedding packages and the Node Admin client.
- Added per-process durable catalog replay and independent provisioning retries
  so every clustered node installs newly defined boundaries without a restart
  and one failed definition does not block later definitions.

### Changed

- Moved Docker Hub publishing and image documentation from `orexza/orisun` to the OrisunLabs-owned `orisunlabs/orisun` repository.
- PostgreSQL schema mappings and SQLite boundary files are reconciled through
  `BoundaryCreated` events marked `existedBeforeCatalog` during the first
  upgraded startup. FoundationDB is beta and does not perform legacy catalog
  migration. New definitions provision backend storage, runtime registries,
  publishers, and dynamic projectors from the same event flow.

### Migration Notes

- Follow the complete
  [0.7.0 to 0.8.0 upgrade guide](docs/docs/operations/upgrading-0.7-to-0.8.md)
  for preflight checks, backend-specific configuration, rolling-upgrade
  sequencing, verification, client changes, and rollback.
- Back up the durable store and admin boundary, remove
  `ORISUN_BOUNDARIES` from the 0.8.0 configuration, and start one upgraded node
  first. If a template is shared with 0.7.0 nodes, retain the setting until the
  last old node is drained.
- Keep all existing PostgreSQL `boundary:schema` mappings in
  `ORISUN_PG_SCHEMAS` during the first startup and any mixed-version rollout.
  SQLite deployments must keep their existing storage directory unchanged for
  discovery.
- Verify every expected entry from `Admin/ListBoundaries` is `ACTIVE` before
  moving traffic or starting the remaining cluster nodes. Inspect `last_error`
  for failed provisioning.
- Replace embedded `MessageHandler[orisun.Event]` subscriptions with an
  `eventstore.EventHandler` callback. Return an error from the callback to stop
  the subscription; use the callback context for cancellation.
- Update direct imports of server-module generated Go types from
  `orisun` to `orisun/grpcapi`. Embedded domain types remain in `orisun` and
  do not depend on generated messages.
## 0.6.1 - 2026-07-18

### Changed

- Updated the project, CI/release workflows, and FoundationDB development/test containers to Go 1.26.5.
- Updated the README and documentation examples for the `v0.6.0` module path, binaries, and versioned container images.

## 0.6.0 - 2026-07-18

### Breaking Changes

- The root Go module has moved from `github.com/oexza/Orisun` to `github.com/OrisunLabs/Orisun`. Go consumers must update module requirements and imports to the new path.

### Migration Notes

- Replace `github.com/oexza/Orisun` with `github.com/OrisunLabs/Orisun` in `go.mod` and Go imports, then run `go mod tidy`.
- Fetch the migrated module with `go get github.com/OrisunLabs/Orisun@v0.6.0`.
- The gRPC wire contract, stored event format, configuration, and runtime behavior are unchanged. Version `v0.5.0` remains available under the former module path for consumers that have not migrated.

### Changed

- Protobuf `go_package` metadata and regenerated Go descriptors now use the OrisunLabs module path.
- Internal imports, embedding examples, build linker paths, release builds, and admin API documentation now consistently use the canonical module identity.
- Go, Node, and Java client submodules now point to the migrated shared protobuf definitions; the standalone client package identities are unchanged.

## 0.5.0 - 2026-07-18

### Breaking Changes

- Backend extensions now implement `EventsSaver.SavePrepared` with a canonical `PreparedEventBatch` and `EventsRetriever.GetBatch` with a contiguous `ReadEventBatch`. Third-party storage backends must migrate from the previous `Save` and protobuf-shaped `Get` methods.
- Embedded Go callers now receive `ReadEventBatch` from `OrisunServer.GetEvents`. Read positions are exposed as scalar `CommitPosition` and `PreparePosition` fields, and `DateCreated` is a `time.Time`. The public gRPC/protobuf contract is unchanged.
- Embedded `OrisunServer.GetLatestByCriteria` callers now pass `LatestByCriteriaQuery` and receive `LatestByCriteriaBatch`. Matches are positionally aligned with the input criteria and expose a `Found` flag plus a packed `ReadEvent`.

### Changed

- Event input is normalized once at the API boundary before storage, eliminating repeated backend JSON decoding.
- Publisher, catch-up, and latest-by-criteria reads stay in packed representations. The publisher validates a complete advancing batch before publishing, uses an allocation-bounded JSON encoder, and persists one checkpoint after the whole batch is acknowledged.
- Read pages are bounded at 10,000 rows. The gRPC API rejects larger page requests, while backend drainers continue paging by position.

## 0.4.10 - 2026-07-13

### Added

- SQLite now uses a per-boundary group-commit write path for `SaveEvents`. Requests are queued per boundary, flushed in batches, and each request still runs its own CCC check and savepoint inside the shared transaction.
- SQLite group-commit coverage now exercises batching, overflow, cancellation, panic recovery, notifier behavior, clean shutdown, direct-mode parity, gap-free concurrent commits, and cross-flush expected-position conflicts.
- Release CI now includes FoundationDB Docker image coverage and backend-specific release artifacts.

### Fixed

- SQLite defaults to `ORISUN_SQLITE_SYNCHRONOUS=FULL` so acknowledged WAL commits are fsynced before success returns. `NORMAL` remains available as an explicit throughput opt-out.
- SQLite CCC concurrency tests now cover concurrent saves against the same content-query context and verify that exactly one writer wins while losing writers receive `ALREADY_EXISTS`.
- SQLite group-commit tests now wait for the current blocker flush to start instead of assuming the single-flush counter begins at zero. This removes the CI timing race in `TestGroupCommit_InBatchSameExpectedPositionOnlyOneWins`.
- FoundationDB release builds now use a Go binding revision compatible with the packaged FoundationDB 7.3 runtime and headers, avoiding the API-800/header mismatch seen in failed release artifact builds.

### Changed

- SQLite stores derived operational state in `{boundary}_metadata.db` files so publisher/projector/admin metadata does not contend with the event-log writer.
- Release validation timeouts were extended for Docker-backed integration suites.
- Public docs and landing pages now include FoundationDB beta in the supported backend list.

## 0.4.0 - 2026-06-11

### Added

- `EventStore/GetLatestByCriteria` RPC: returns the latest event per criterion assembled from one consistent server-side read snapshot, plus a `context_position` for use as the next `expected_position`. Closes the mixed-snapshot gap where a context assembled from independent `GetEvents` calls can miss an event that committed between them below the observed max position. Implemented for PostgreSQL (single-statement `UNION ALL` of `LIMIT 1` lookups) and SQLite (one deferred read transaction).
- General-ledger workload e2e test (`TestE2E_LedgerWorkload_*`) driving concurrent double-entry transfers with carried balances through the public gRPC API, auditing carried-balance consistency, no-overdraft, money conservation, and debit/credit pairing.
- FoundationDB storage backend (`ORISUN_BACKEND=foundationdb`, built with `-tags foundationdb`). Event positions come from FoundationDB commit versionstamps, so plain appends and commands on different aggregates commit in parallel without a per-boundary sequence counter on the write path. Criteria reads and consistency conditions both require a ready covering index and fail closed (`FAILED_PRECONDITION`) otherwise; conflict ranges are scoped to the matching index slice. System indexes covering Orisun's own admin queries are created on the admin boundary at startup.
- `ORISUN_FDB_TRANSACTION_TIMEOUT_MS` (default `10000`) and `ORISUN_FDB_TRANSACTION_RETRY_LIMIT` (default unlimited) bound FoundationDB transactions.
- `scripts/fdb_test_container.sh` runs the FoundationDB backend tests against a throwaway single-node Docker cluster.

### Fixed

- PostgreSQL positions are now assigned in commit order per boundary: `insert_events_with_consistency_v3` takes a per-boundary advisory lock from position draw until commit. Previously a concurrent batch could draw lower `global_id`s, stay in flight while a later-drawn batch committed, and then commit below a context max another writer had already observed. A scalar expected-position check could not detect that case. The lock closes that window; the cost is that writes within one boundary serialise from draw to commit (the SQLite and FoundationDB backends already had commit-ordered positions). The function is replaced automatically at startup; no data migration.

## 0.3.1 - 2026-06-06

### Fixed

- Preserved pre-`0.3.0` read and subscription cursor semantics for PostgreSQL-backed stores. Clients can continue using `{0, 0}` as the beginning cursor for `GetEvents` and `CatchUpSubscribeToEvents`.
- PostgreSQL logical `commit_position` values are now derived as `MAX(global_id) + 1` for each committed batch, so no event is emitted at the exact `{0, 0}` position while existing zero-based `prepare_position` values remain unchanged.
- PostgreSQL startup migration now remaps legacy publisher and projector checkpoints to the corrected logical commit positions without changing stored `global_id` / `prepare_position` values.

## 0.3.0 - 2026-06-06

### Breaking Changes

- PostgreSQL event positions no longer expose PostgreSQL internal transaction IDs as `commit_position`.
  Existing PostgreSQL event rows are migrated on startup so `transaction_id` becomes a durable Orisun logical commit position derived from event `global_id` ordering. PostgreSQL's `pg_current_xact_id()` is still stored internally in `pg_xact_id` for stable-prefix visibility checks, but it is no longer part of the public EventStore position.
- Existing PostgreSQL projector checkpoints and publisher checkpoints are remapped to the new logical positions during startup migration. Consumers that persisted Orisun positions outside the Orisun database should rebuild or remap those external checkpoints before resuming from them.

### Migration Notes

- Stop Orisun cleanly before upgrading so publisher and projector checkpoints are current.
- Back up PostgreSQL before first starting Orisun `0.3.0`.
- Start one Orisun `0.3.0` node first and allow migrations to complete for all configured boundaries before starting the rest of a cluster.
- PostgreSQL major upgrades no longer need to preserve PostgreSQL transaction IDs for Orisun correctness after this migration. `pg_xact_id` is treated as current-cluster-only metadata and stale values are cleared when detected.

### Fixed

- PostgreSQL subscriptions and publishers no longer depend on PostgreSQL transaction IDs remaining monotonic across major upgrades, dump/restore migrations, logical replication moves, or fresh cluster restores.
- Missing projector checkpoints now resume from the before-first position instead of `(0, 0)`, preventing first logical event skips.
- Added Postgres live-subscription E2E coverage for write-after-subscribe delivery.
