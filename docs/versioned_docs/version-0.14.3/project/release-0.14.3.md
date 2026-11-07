---
title: Release v0.14.3
description: Shared group-commit lifecycle for PostgreSQL and SQLite, plus dependency updates.
---

## Changes

PostgreSQL and SQLite group commit now share one coordinator for bounded
admission, batching, cancellation, flush timeouts, completion, and shutdown.
Each boundary keeps a single ordered writer. Caller cancellation cannot control
a shared transaction, and results reach callers only after commit.

Request preparation now happens before writer ownership. PostgreSQL prepares
payloads and consistency plans before taking the advisory lock, and SQLite
prepares consistency queries and application JSON before the writer
transaction. CCC semantics are unchanged: checks run in request order, and
savepoint isolation, error precedence, and exact JSON numeric values are
preserved.

Dependencies are updated, including `nats.go` 1.55.0, `golang.org/x/crypto`
0.58.0, `golang.org/x/net` 0.61.0, and `modernc.org/sqlite` 1.61.0.

## Upgrade

Public APIs, configuration, and storage schema versions are unchanged from
v0.14.2. Upgrading requires no storage migration or checkpoint changes.
PostgreSQL startup replaces the installed write functions with this release's
version; their signatures are unchanged.

## Distribution

PostgreSQL-only and SQLite-only binaries are available for Linux, macOS, and
Windows. Docker Hub and GHCR publish `0.14.3-pg` and `0.14.3-sqlite` images,
alongside the `pg` and `sqlite` channel tags.
