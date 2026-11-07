---
title: Storage Upgrade Policy
description: Upgrade seamlessly from the immediately preceding storage version.
---

## Supported upgrade

Startup upgrades storage from the `0.13.0` and `0.14.0` releases automatically.
Stop all old server nodes first, retain a backup, then start one upgraded node and
wait for every catalogued boundary to initialize before starting the remaining nodes.
The upgrade preserves event documents, IDs, positions, write contexts, index definitions,
sequence values, and projector checkpoints. No export/import or cursor reset is
needed for this upgrade.

| Backend | Previous release | Current storage marker |
| --- | --- | --- |
| PostgreSQL | v0.13.0: per-boundary schema version `4`; v0.14.0: version `5`. | Schema version `5`; removes the obsolete publisher checkpoint table when upgrading from version `4`. |
| SQLite | v0.13.0: event version `6`, metadata version `1`; v0.14.0: event version `7`, metadata version `2`. | Event version `8`, metadata version `2`; repairs the built-in event-type index and removes the obsolete publisher checkpoint table when upgrading metadata version `1`. |

PostgreSQL upgrades commit with boundary initialization. SQLite upgrades commit
inside a savepoint.
Interrupted transactions leave the previous markers intact, so startup can retry.
SQLite event versions `6` and `7` upgrade atomically to `8` by rebuilding only the built-in event-type index with the same scalar JSON expression used by reads and CCC. Application-managed indexes, event documents, write contexts and positions remain unchanged. Fresh databases create the corrected index directly. The runtime uses only current
documents; it does not resume unfinished old conversion jobs or serve multiple
event formats.

Events saved before write-context recording retain their missing evidence: their
`write_id` is empty, and no consistency observations are fabricated. New writes
always persist their actual context.

Older versions and unversioned PostgreSQL/SQLite stores are rejected without rewriting their events. For these stores,
use the export/import procedure below.

Do not change version markers to bypass validation. The marker certifies the
physical schema and document format; changing it does not convert the data.

## Older stores

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

## Rollout and verification

1. Record every catalogued boundary, its placement, counts, representative event
   IDs and positions, managed indexes, and application/projector checkpoints.
2. Stop all servers and embedded stores sharing the databases. Take a restorable
   backup of the admin catalog and every application boundary. For SQLite, back
   up both the event file and metadata file consistently; do not copy a live WAL
   database file alone.
3. Update clients before traffic resumes. SDK `SaveEvents` remains available,
   but old SDK binaries still invoke the removed server RPC. Stop older server
   nodes; a mixed-version notification rollout is unsupported.
4. Start one upgraded node. Verify gRPC readiness, active catalog entries, and
   absence of storage initialization failures before adding the other nodes.
5. Compare events, positions, write contexts, indexes, and checkpoints with the
   recorded baseline. Test a valid CCC save and a stale observation rejection,
   then reconnect a consumer from a retained checkpoint and verify ordered replay.
6. Confirm Core NATS and JetStream are healthy. All subscription events now come
   from backend reads; tune the read pools and explicit indexes for that load.

## Failure recovery and rollback

Initialization is transactional per PostgreSQL boundary and per SQLite database;
there is no installation-wide migration transaction. A failure may leave some
boundaries upgraded and others on the supported previous marker. Fix the cause
and restart this release; successfully upgraded databases reopen normally.

Older binaries reject the new markers. To roll back, stop every upgraded node
and restore the complete pre-upgrade backup, including the admin catalog and
application databases. Do not lower markers manually. Restoring the backup
also discards writes accepted after the backup; decide how to preserve or replay
those application events before resuming traffic.
