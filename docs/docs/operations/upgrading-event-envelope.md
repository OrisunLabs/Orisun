---
title: Storage Upgrade Policy
description: The breaking release initializes current storage and rejects older formats.
---

This is a breaking release. Fresh stores initialize the current event-document
format directly. Existing databases and key ranges in older formats are rejected;
startup does not transform event data, rewrite consistency contexts, rebuild
historical indexes, or resume conversion jobs.

| Backend | Supported storage marker |
| --- | --- |
| PostgreSQL | Per-boundary schema version `5`. |
| SQLite | Event database `PRAGMA user_version = 7`; metadata database version `1`. |
| FoundationDB | Per-boundary schema version `1` under the configured root. |

Do not change version markers to bypass validation. The marker certifies the
physical schema and document format; changing it does not convert the data.

For an older deployment, retain its backup and use its matching binary to export
application events. Initialize a fresh store with this release, create boundaries
and indexes through the current APIs, then import those application events using
supported client write methods. Include event IDs, types, application data, and
metadata; do not submit storage-owned top-level `__*` fields as application data.

An API import assigns new positions and write IDs. Rebuild projections and their
checkpoints, obtain fresh CCC observations, and establish new subscription cursors.
No automatic migration tool is provided by this release.

Restoring a backup already in the supported format retains its positions and
write contexts. Restore the admin catalog alongside the application boundaries;
startup installs active placements from that catalog. `CreateBoundary` can attach
current-format storage restored into a new deployment.

Client `SaveEvents` remains supported with its existing single-query shape. Updated
clients translate it into the same `SaveEventsV2` RPC used for multi-observation
writes. The old server write RPC and deprecated annotations are removed.
