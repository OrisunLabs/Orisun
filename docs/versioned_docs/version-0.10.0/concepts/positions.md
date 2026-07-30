---
title: Positions and Ordering
description: How Orisun numbers committed events and how to use positions for consistency and paging.
---

Every committed event has a durable **position**. Positions are how Orisun expresses ordering, optimistic concurrency, and read paging. Understanding them makes the rest of the API predictable.

## The position pair

A position has two fields:

| Field | Backed by | Meaning |
| --- | --- | --- |
| `commit_position` | `transaction_id` | Groups events committed together. Every event saved in one `SaveEvents` batch shares the same `commit_position`. |
| `prepare_position` | `global_id` | The per-boundary monotonic sequence of the individual event. Unique and strictly increasing within a boundary. |

Ordering within a boundary is the tuple `(commit_position, prepare_position)`, ascending. Positions are per boundary, so they are not comparable across boundaries.

Treat positions as opaque ordering tokens. Some backends store dense integer sequences; FoundationDB encodes commit versionstamp data into the same `int64` pair, so values remain ordered and unique but are not guaranteed to be gap-free.

## PostgreSQL transaction IDs

In Orisun `0.3.1` and later, PostgreSQL `transaction_id` is an Orisun logical commit position, not PostgreSQL's internal transaction ID. PostgreSQL's `pg_current_xact_id()` is still recorded internally as `pg_xact_id` so the PostgreSQL backend can avoid publishing or reading past open older transactions, but that value is current-cluster metadata and is not exposed as the public EventStore position.

This matters during PostgreSQL major upgrades and restore workflows. PostgreSQL internal transaction IDs are assigned by a cluster-local counter. A `pg_upgrade` path may preserve enough cluster state for continuity, but dump/restore, logical replication moves, and some managed-service migrations can create a fresh cluster with lower internal transaction IDs. Orisun positions must remain valid across those workflows, so public positions use logical event-store ordering instead.

The `0.8.0` bridge release includes the migrations that remap older
`transaction_id` values to logical commit positions and update publisher and
projector checkpoints that point at stored events. Installations older than
`0.8.0` must run that release before upgrading further. Current releases still
treat `pg_xact_id` values from a previous cluster as disposable visibility
metadata and clear them when detected as stale.

## Empty and beginning positions

Use `{-1, -1}` as the empty expected position for writes:

```json
{"commit_position": -1, "prepare_position": -1}
```

As `expected_position` in `SaveEvents`, it asserts the consistency context is still empty.

Use `{0, 0}` as the beginning cursor for reads and subscriptions:

```json
{"commit_position": 0, "prepare_position": 0}
```

No event is assigned the exact position `{0, 0}`. The first event in a boundary can have `prepare_position` `0`, but its `commit_position` is greater than `0`.

## Batch semantics

`SaveEvents` is atomic. For a batch of N events:

- all N share one `commit_position`,
- each gets an increasing `prepare_position`,
- the `WriteResult.log_position` returns the position of the batch, which is the value you pass as the next `expected_position`.

This is why a single account, processed one command at a time, advances through ordered positions while `prepare_position` identifies the event within that ordering. Do not rely on positions increasing by exactly one.

## Positions and consistency

[Command Context Consistency](./command-context-consistency) uses positions as the optimistic-lock token. You read a context, remember the position returned by that context read, and pass it as `expected_position` on the next write. If any newer event matched the context, the save is rejected with `ALREADY_EXISTS`.

For a `GetEvents` history read, the context position is the position of the last event returned. For `GetLatestByCriteria`, use the response `context_position`; it is computed from one server-side read snapshot across all criteria. Independent reads do not provide the same guarantee for a multi-criterion command context.

PostgreSQL serializes position assignment per boundary from position draw through commit. That keeps public positions commit-ordered, so an observed context position is a valid stable upper bound for later consistency checks. SQLite naturally has one writer per boundary file.

## Positions and paging

`GetEvents` reads a bounded page. To walk a boundary or a criteria set, page forward using the position of the last event you received:

1. Call `GetEvents` with `from_position` (`{0, 0}` for the first page) and a `count`.
2. Process the returned events.
3. Use the `position` of the last event as the `from_position` for the next call.
4. Stop when a page returns fewer events than `count`.

Reads return a stable committed prefix, so a page never contains events that a later page should have ordered before it. Because delivery and paging boundaries can overlap a position, keep consumers idempotent and deduplicate by `event_id` rather than assuming each event is seen exactly once.

## Subscriptions

`CatchUpSubscribeToEvents` takes an `after_position`. It replays stored events ordered by position, then transitions to live delivery. A projector should persist the position of the last event it durably processed and resume from that position on restart. See [Delivery Guarantees](./delivery-guarantees).
