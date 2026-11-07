---
title: Release v0.14.0
description: Tag comparison operators, backend-owned subscriptions, and the supported upgrade from v0.13.0.
---

## Highlights

- Content-query tags now support `eq`, `ne`, `gt`, `gte`, `lt`, and `lte`. Omitted or empty operators retain equality behavior. Ordered comparisons use the stored JSON type: numbers compare exactly and strings compare in UTF-8 byte order. Missing and null fields never match. Repeated-field predicates allow ranges such as `amount >= 10 AND amount < 20`.
- Subscriptions fetch every delivered event from the durable backend. Core NATS carries empty boundary wake-up hints; JetStream continues to serve leases and admin messaging. Historical catch-up and newly committed events use the same backend criteria evaluation and position ordering.
- Subscription cursors advance only after successful handler delivery. Whole-batch validation protects ordering, and inclusive reads preserve cursor progress without timestamp overlaps. A subscription without an `after_position` delivers the latest matching event, then continues forward.
- PostgreSQL and SQLite upgrade storage from v0.13.0 automatically at startup, preserving event IDs, positions, write contexts, indexes, sequences, and projector checkpoints. The upgrade removes obsolete publisher checkpoint tables without rebuilding event documents or physical indexes.
- The shared PostgreSQL/SQLite operator matrix covers 278 scenarios per backend, including every operator pair, AND/OR combinations, repeated fields, exact numeric boundaries, typed edge cases, paging, CCC conflicts, and durable write contexts. Additional group-commit tests verify every operator against earlier accepted writes in the same batch.

## Breaking changes

- FoundationDB is no longer a supported backend. Its configuration, binary, embedding package, Docker flavor, and build scripts have been removed. Existing FoundationDB deployments must migrate to PostgreSQL or SQLite before adopting this release; this release does not perform a cross-backend conversion.
- `SaveEventsV2` is the sole server write RPC. Applications calling the legacy `SaveEvents` RPC must upgrade to an updated client or call `SaveEventsV2`. Updated Go, Node, and Java clients retain their single-query `SaveEvents` methods and translate them to the canonical RPC.
- Embedded Go appends now carry `Consistency` observations, each pairing a complete content query with its observed position. The former `ExpectedPosition`/`Subset` append fields and legacy full-event publisher APIs are removed. Custom lock providers implement the explicit `AcquireLock` lease lifecycle.
- NATS event-payload streams and publisher checkpoints are no longer part of subscription delivery. Remove `ORISUN_POLLING_PUBLISHER_BATCH_SIZE` and `ORISUN_NATS_EVENT_STREAM_MAX_BYTES`, `ORISUN_NATS_EVENT_STREAM_MAX_MSGS`, and `ORISUN_NATS_EVENT_STREAM_MAX_AGE` from deployment configuration.
- Startup supports only the immediately preceding storage formats: PostgreSQL schema version 4 and SQLite event version 6 / metadata version 1, as shipped in v0.13.0. Older or unversioned stores require export/import into a fresh store.

## Upgrade from v0.13.0

1. Back up the complete installation, including the admin catalog and every application database.
2. Stop all old server nodes. Do not perform a rolling mixed-version upgrade.
3. Start one v0.14.0 node and wait for every catalogued boundary to initialize. PostgreSQL moves to schema version 5; SQLite moves to event version 7 and metadata version 2.
4. Verify readiness, active boundaries, reads, CCC writes, and subscriptions, then start the remaining upgraded nodes. Existing backend positions and consumer checkpoints require no remapping for this supported upgrade.

Upgrade transactions are scoped to each PostgreSQL boundary or SQLite database. Interrupted initialization leaves that database's previous markers intact; fix the cause and restart. Older binaries reject the new markers. Rollback requires stopping upgraded nodes and restoring the complete pre-upgrade backup; lowering markers manually is unsupported.

See the [storage upgrade policy](https://orisunlabs.github.io/Orisun/docs/operations/upgrading-event-envelope) for export/import and rollback details.

## Subscription operations

Healthy NATS is required for live wake-ups and recovery. Each subscription's idle watchdog publishes a boundary hint after `ORISUN_SUBSCRIPTION_IDLE_THRESHOLD` of silence, defaulting to `10s`; a received hint triggers a backend drain. The watchdog does not periodically read the backend directly. Consumers retain their own durable checkpoints and must handle at-least-once replay.

Subscription reads increase backend query load. Use selective criteria, explicit indexes, and appropriately sized read pools. SQLite remains single-node and incompatible with NATS clustering.

## Distribution and validation

The release provides PostgreSQL-only and SQLite-only binaries for Linux, macOS, and Windows. Docker Hub and GHCR publish versioned `0.14.0-pg` and `0.14.0-sqlite` images alongside the `pg` and `sqlite` channel tags. Go builds use Go 1.27.2.

The full Go test suite, final PostgreSQL/SQLite operator race tests, and focused `go vet` checks passed before release preparation. The release workflow requires successful CI for the tagged commit before building and publishing artifacts. Versioned documentation is built and published with the same release commit.
