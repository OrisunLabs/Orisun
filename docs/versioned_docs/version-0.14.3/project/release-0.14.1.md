---
title: Release v0.14.1
description: Repair SQLite's built-in event-type index for content queries and CCC.
---

## Fix

SQLite's built-in event-type index now uses the same scalar JSON expression as
content-query reads and CCC checks. This lets SQLite use the index for event-type
lookups, including absent event types and queries with runtime-keyed fields,
without sorting through a temporary B-tree. Fresh databases create the corrected
index directly.

## Upgrade

SQLite event schema versions 6 (v0.13.0) and 7 (v0.14.0) upgrade automatically to
version 8 at startup. The migration atomically rebuilds only the built-in
event-type index. Application-managed indexes, event documents, IDs, positions,
write contexts, sequences, and projector checkpoints are preserved. Metadata
remains at version 2; v0.13.0 metadata upgrades from version 1 as before.

Back up the complete installation and stop existing processes before upgrading.
SQLite remains single-node. Verify reads, CCC writes, and subscription replay
after startup. SQLite v0.14.0 rejects the new event schema marker; rollback
requires stopping the upgraded process and restoring the pre-upgrade backup.
Do not lower schema markers manually.

PostgreSQL storage and public APIs are unchanged. The supported PostgreSQL
upgrade from v0.13.0 remains available. See the
[storage upgrade policy](https://orisunlabs.github.io/Orisun/docs/operations/upgrading-event-envelope)
for the full upgrade and rollback procedure.

## Distribution

PostgreSQL-only and SQLite-only binaries are published for Linux, macOS, and
Windows. Docker Hub and GHCR publish `0.14.1-pg` and `0.14.1-sqlite` images,
alongside the `pg` and `sqlite` channel tags.
