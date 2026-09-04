---
title: Upgrading from 0.9 to 0.10
description: Prepare supported backends and YugabyteDB deployments for the 0.10.0 breaking release.
---

Orisun 0.10.0 removes YugabyteDB support and the `ORISUN_PG_DIALECT`
configuration. The PostgreSQL backend now targets PostgreSQL only.

## PostgreSQL, SQLite, and FoundationDB

The YugabyteDB removal does not require a storage migration for deployments
already using PostgreSQL, SQLite, or FoundationDB. Before upgrading:

1. Back up the durable event store and admin boundary.
2. Remove `ORISUN_PG_DIALECT` from every process, container, secret, and
   deployment manifest. Orisun 0.10.0 rejects the removed setting instead of
   silently ignoring it.
3. Review the [0.10.0 changelog](https://github.com/OrisunLabs/Orisun/blob/main/CHANGELOG.md)
   for the PostgreSQL group-commit and configuration changes.
4. Deploy and verify readiness, event reads, CCC writes, publisher checkpoints,
   and subscriptions before completing the rollout.

PostgreSQL startup applies the current boundary initializer before registering
catalog boundaries, so the 0.10.0 SQL functions are installed automatically.

## YugabyteDB

Do not point an Orisun 0.10.0 process at YugabyteDB. The Yugabyte-specific SQL,
committed-position watermark, migrations, and runtime dialect have been
removed.

There is no supported in-place YugabyteDB-to-PostgreSQL migration in 0.10.0.
Choose one of these paths before upgrading:

- remain on the latest 0.9.x release while planning a migration; or
- migrate the complete event log, admin boundary, catalog, index definitions,
  and publisher/projector checkpoints to PostgreSQL, SQLite, or FoundationDB
  using a separately validated migration process.

An application-level event replay is not an exact-copy migration: it assigns
new positions and requires downstream checkpoints and position-bearing read
models to be rebuilt. Validate event counts, ordering, event IDs, boundary
definitions, users, indexes, and checkpoints before directing production
traffic to the replacement backend.
