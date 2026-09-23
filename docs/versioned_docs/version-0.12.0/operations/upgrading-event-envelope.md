---
title: Upgrading the Event Envelope
description: Upgrade existing storage to reserved event fields with backend-owned encoding and retrieval.
---

This guide applies when upgrading to Orisun 0.12.0 from 0.11.x or an earlier
supported storage format. New empty stores initialize automatically; existing
stores migrate when their boundaries are initialized. Installations older than
0.8.0 must first complete the [0.8.0 bridge upgrade](./upgrading-0.7-to-0.8).

The public event shape stays the same. PostgreSQL and SQLite move queryable
envelope values into `data`; FoundationDB stores the fields available before
commit and derives positions and write IDs from its native keys. See the
[event field reference](../api/eventstore#data-model) for the document model.

## Prepare

1. Rehearse the upgrade and restore procedure against a recent backup. Allow
   downtime and enough storage for table and index rebuilding on SQL backends.
2. Inventory every boundary, including the admin boundary. Record event counts,
   latest positions, index definitions, and representative historical events
   and write contexts for comparison after migration.
3. Update application query criteria, subscription filters, and index
   declarations from `eventType` to `__eventType`. Keep the API `event_type`
   field and SDK `eventType` property unchanged. No query-time alias translates
   the old key.
4. Remove top-level `__*` keys from new application payloads. The namespace is
   reserved; nested objects and metadata may still contain such keys.
5. Review SQL-created indexes, views, triggers, and external queries that
   depend on the old physical columns or `eventType` JSON key. Managed index
   definitions are migrated automatically, but arbitrary SQL dependencies are
   not rewritten. Event IDs move out of the separate `event_id` column, and
   generated envelope columns cannot be assigned by direct SQL writes.
6. On FoundationDB, remove secondary indexes whose fields or conditions use
   `__commitPosition`, `__preparePosition`, or `__writeId` before upgrading.
   Queries use native commit or write-ID ranges instead; prepare position
   needs one of those anchors. Such indexes block the envelope migration.

Legacy application data can collide with the destination names. The migration
rejects an existing `__eventId`, or any of `__commitPosition`,
`__preparePosition`, `__writeId`, `__dateCreated`, and `__metadata` when the
corresponding migration stage runs. FoundationDB also checks
`__writeLastOffset`. The event-type migration rejects a document containing
both `eventType` and `__eventType`. JSON null still counts as an existing key.
Resolve application-owned collisions without discarding their values; also
update the application queries that refer to those values. Do not rename
system fields in a store that has already completed the relevant stage.

## Upgrade

1. Pause application traffic and stop every Orisun process sharing the store,
   including embedded instances. Do not run old and new binaries together.
2. Take a recoverable backup of the complete stopped deployment, including
   admin/catalog data, application boundaries, retained write contexts,
   checkpoints, and durable NATS state. For SQLite, include both event and
   metadata databases and any WAL state needed by the chosen backup method.
3. Deploy the new binary and start one Orisun process with the same storage
   configuration and boundary placements. No separate migration command is
   required. Keep application traffic paused while boundaries initialize.
4. Wait for the admin boundary and every expected application boundary to
   initialize successfully. Investigate migration errors before starting
   additional nodes or resuming traffic.
5. Complete the checks below, then start the remaining PostgreSQL or
   FoundationDB nodes using the same binary. SQLite remains single-node.
6. Resume traffic with updated criteria. Re-read CCC contexts before issuing
   writes; do not reuse in-flight observations expressed with the old key.

## Backend behavior

| Backend | Migration behavior |
| --- | --- |
| PostgreSQL | Migrates each boundary transactionally, takes an exclusive event-table lock, and rebuilds indexes around generated stored columns. Internal `pg_xact_id` visibility bookkeeping remains separate. |
| SQLite | Applies versioned migration steps transactionally. The envelope step copies events in bounded batches, swaps tables, and recreates SQL-defined indexes and triggers. Envelope projections are virtual generated columns. |
| FoundationDB | Commits transformed records and resume cursors together in bounded batches. A boundary is unavailable until its storage migration completes. A restart resumes completed work and maintains affected secondary indexes. |

Migration is not one atomic operation across all boundaries. SQLite can also
retain earlier completed migration steps if a later step fails. FoundationDB
can retain completed batches within a stage. Do not infer that the entire
store is unchanged from a failed startup.

Positions, write IDs, and publisher checkpoints are preserved. Historical
events without retained write contexts continue to return an empty write ID.
The migration does not reconstruct consistency observations that were never
recorded.

## Verify before resuming traffic

- Confirm all expected catalog boundaries initialized and their indexes are
  available. FoundationDB secondary-index queries require ready indexes.
- Compare event counts, latest positions, historical event IDs, metadata,
  timestamps, and recorded write contexts with the pre-upgrade inventory.
- Read events through the API: ordinary envelope fields must be populated,
  returned application `data` must contain no top-level `__*` fields, and
  nested application fields must remain intact.
- Exercise representative `__eventType` queries, pagination in both
  directions, and latest-event reads. On FoundationDB, also check any native
  commit-position or write-ID criteria used by the application.
- Use a test boundary to verify an accepted write, a rejected stale CCC
  observation, and subscription delivery. Confirm publishers and projectors
  resume from their retained checkpoints without ordering errors.

## Failure and rollback

Keep traffic paused if migration fails. Inspect the failing boundary and
stage, correct the reported collision or dependency with all Orisun processes
stopped, and restart the new binary. Preserve migration version records and
FoundationDB resume markers; resetting them can reapply transformations to
already migrated data.

There is no reverse migration. To return to an older binary, stop all Orisun
processes and restore the complete pre-upgrade deployment backup before
starting that binary. Do not point an old binary at partially or fully
migrated storage. Restoring the backup discards writes accepted afterward,
which is why verification should finish before production traffic resumes.
