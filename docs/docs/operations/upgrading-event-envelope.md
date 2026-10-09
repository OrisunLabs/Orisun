---
title: Storage Upgrade Policy
description: Upgrade seamlessly from the immediately preceding storage version.
---

Startup upgrades storage from the `0.13.0` release automatically.
Stop all old server nodes first, retain a backup, then start one upgraded node and
wait for every catalogued boundary to initialize before starting the remaining nodes.
The upgrade preserves event documents, IDs, positions, write contexts, index definitions,
sequence values, and projector checkpoints. No export/import or cursor reset is
needed for this upgrade.

| Backend | Previous release | Current storage marker |
| --- | --- | --- |
| PostgreSQL | Per-boundary schema version `4`. | Schema version `5`; removes the obsolete publisher checkpoint table. |
| SQLite | Event version `6`; metadata version `1`. | Event version `7`, metadata version `2`; removes the obsolete publisher checkpoint table. |
| FoundationDB | Completed `reserved_event_type`, `event_id_document`, and `envelope_document` stages. | Per-boundary schema version `1`; replaces completed progress markers. |

PostgreSQL upgrades commit with boundary initialization. SQLite upgrades commit
inside a savepoint. FoundationDB replaces completed progress markers atomically.
Interrupted transactions leave the previous markers intact, so startup can retry.
No physical indexes or event documents are rebuilt. The runtime uses only current
documents; it does not resume unfinished old conversion jobs or serve multiple
event formats.

Events saved before write-context recording retain their missing evidence: their
`write_id` is empty, and no consistency observations are fabricated. New writes
always persist their actual context.

Older versions, unversioned PostgreSQL/SQLite stores, and incomplete FoundationDB
conversion stages are rejected without rewriting their events. For these stores,
use the export/import procedure below.

Do not change version markers to bypass validation. The marker certifies the
physical schema and document format; changing it does not convert the data.

For an older deployment, retain its backup and use its matching binary to export
application events. Initialize a fresh store with this release, create boundaries
and indexes through the current APIs, then import those application events using
supported client write methods. Include event IDs, types, application data, and
metadata; do not submit storage-owned top-level `__*` fields as application data.

An API import assigns new positions and write IDs. Rebuild projections and their
checkpoints, obtain fresh CCC observations, and establish new subscription cursors.
This export/import procedure is only needed for storage older than the supported upgrade source.

Restoring a backup already in the supported format retains its positions and
write contexts. Restore the admin catalog alongside the application boundaries;
startup installs active placements from that catalog. `CreateBoundary` can attach
current-format storage restored into a new deployment.

Client `SaveEvents` remains supported with its existing single-query shape. Updated
clients translate it into the same `SaveEventsV2` RPC used for multi-observation
writes. The old server write RPC and deprecated annotations are removed.
