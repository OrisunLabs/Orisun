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
| SQLite | v0.13.0: event version `6`, metadata version `1`; v0.14.0: event version `7`, metadata version `2`; v0.14.1 to v0.14.3: event version `8`. | Event version `9`, metadata version `2`; moves envelope fields into typed columns and removes the obsolete publisher checkpoint table when upgrading metadata version `1`. |

PostgreSQL upgrades commit with boundary initialization. SQLite upgrades commit
inside a savepoint.
Interrupted transactions leave the previous markers intact, so startup can retry.
SQLite event versions `6`, `7`, and `8` upgrade atomically to `9`. The upgrade
rewrites every event: envelope fields move to typed columns, and `data` keeps only
application fields. Event IDs, positions, write contexts, application values, and
metadata are preserved, and criteria on reserved fields select the same events.
All indexes are rebuilt, with managed indexes regenerated from their stored
definitions.

SQLite upgrade time grows linearly with boundary size and runs before the server
accepts traffic. As a guide, 5 million events of roughly 1 KB each (a 3.4 GB
event database) took about two minutes on an Apple M1 Pro SSD. Measure a copy of
your largest boundary before planning the maintenance window.

Disk and memory during the SQLite upgrade:

- Keep free disk space of about 2.5 times the event database size on its volume.
  Old and new tables coexist until commit, and the rewrite passes through the WAL.
- Index rebuilds sort on disk. The upgrade temporarily overrides
  `ORISUN_SQLITE_TEMP_STORE` with `FILE`, so memory stays small regardless of
  boundary size, then restores the configured value. The sort files go to
  SQLite's temporary directory: `SQLITE_TMPDIR`, else `TMPDIR`, else `/var/tmp`,
  else `/tmp`. If that directory is a memory-backed `tmpfs`, as in many
  containers, the sort consumes memory again; point `SQLITE_TMPDIR` at a
  disk-backed directory for the first start.
- The database file does not shrink afterward, growing to about 1.5 times its
  previous size; run `VACUUM` offline to reclaim the space.

The upgrade refuses to run, changing nothing, if an event holds an unknown
top-level `__*` field or a write ID that disagrees with its position. Earlier
binaries reject event version `9`, so rollback requires restoring the pre-upgrade
backup.

The runtime uses only the current format; it does not resume unfinished old
conversion jobs or serve multiple event formats.

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
