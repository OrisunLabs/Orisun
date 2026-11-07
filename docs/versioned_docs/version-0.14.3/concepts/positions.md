---
title: Positions and Ordering
description: How Orisun numbers committed events and how to use positions for consistency and paging.
---

Every committed event has a durable **position**. Positions are how Orisun expresses ordering, optimistic concurrency, and read paging. Understanding them makes the rest of the API predictable.

## The position pair

A position has two fields:

| Field | Backed by | Meaning |
| --- | --- | --- |
| `commit_position` | `transaction_id` | Groups events committed together. Every event saved in one `SaveEventsV2` batch shares the same `commit_position`. |
| `prepare_position` | `global_id` | The per-boundary monotonic sequence of the individual event. Unique and strictly increasing within a boundary. |

Ordering within a boundary is the tuple `(commit_position, prepare_position)`, ascending. Positions are per boundary, so they are not comparable across boundaries.

Treat positions as opaque ordering tokens. Values are ordered and unique within a boundary, but need not be gap-free.

## PostgreSQL transaction IDs

In Orisun `0.3.1` and later, PostgreSQL `transaction_id` is an Orisun logical commit position, not PostgreSQL's internal transaction ID. PostgreSQL's `pg_current_xact_id()` is still recorded internally as `pg_xact_id` so the PostgreSQL backend can avoid publishing or reading past open older transactions, but that value is current-cluster metadata and is not exposed as the public EventStore position.

This matters during PostgreSQL major upgrades and restore workflows. PostgreSQL internal transaction IDs are assigned by a cluster-local counter. A `pg_upgrade` path may preserve enough cluster state for continuity, but dump/restore, logical replication moves, and some managed-service migrations can create a fresh cluster with lower internal transaction IDs. Orisun positions must remain valid across those workflows, so public positions use logical event-store ordering instead.

Older storage formats are unsupported. Current storage preserves logical positions
when restored into another PostgreSQL cluster; initialization clears stale
cluster-local visibility markers when necessary.

## Empty and beginning positions

Use `{-1, -1}` as the empty observed position for writes:

```json
{"commit_position": -1, "prepare_position": -1}
```

As a `SaveEventsV2` observation position, it asserts that observation's query is still empty.

Use `{0, 0}` as the beginning cursor for reads and subscriptions:

```json
{"commit_position": 0, "prepare_position": 0}
```

No event is assigned the exact position `{0, 0}`. The first event in a boundary can have `prepare_position` `0`, but its `commit_position` is greater than `0`.

## Batch semantics

`SaveEventsV2` is atomic. For a batch of N events:

- all N share one `commit_position`,
- each gets an increasing `prepare_position`,
- the `WriteResult.log_position` returns the position of the last event in the batch.

This is why a single account, processed one command at a time, advances through ordered positions while `prepare_position` identifies the event within that ordering. Do not rely on positions increasing by exactly one.

## Positions and consistency

[Command Context Consistency](./command-context-consistency) uses query-level observations as optimistic-lock tokens. You preserve each context query with its latest matching position and pass the observations to `SaveEventsV2`. If any query no longer has exactly that latest position, the save is rejected with `ALREADY_EXISTS`.

For a complete `GetEvents` history read, the observed position is the greatest position among the matching events. `GetLatestByCriteria` returns the latest matching `context_position` for its complete criteria list from one snapshot. Pair that position with the exact request criteria to construct a V2 observation. Separate complete reads can each contribute an observation because `SaveEventsV2` validates every one atomically.

Do not substitute `WriteResult.log_position`, a boundary head, or the newest
event from an unrelated query. A position is meaningful for CCC only when it
is paired with the exact query whose latest match it describes.

PostgreSQL serializes position assignment per boundary from position draw through commit. That keeps public positions commit-ordered, so an observed context position is a valid stable upper bound for later consistency checks. SQLite naturally has one writer per boundary file. Group commit can put several saves in one physical transaction; each save still has its own logical commit position and write ID.

## Positions and paging

`GetEvents.from_position` is **inclusive** in both directions: ascending reads
include positions greater than or equal to it, and descending reads include
positions less than or equal to it. Omit it to read from the beginning in `ASC`
or from the latest match in `DESC`.

For forward paging:

1. Use `{0, 0}` for the first cursor and request between 2 and 10,000 rows.
2. Discard only the first row if its complete position equals the cursor.
3. Process the remaining rows in order and use the last returned position as
   the next cursor. Do not increment either position component.
4. Stop when the **original** page contains fewer rows than requested, before
   subtracting the cursor row. A page containing only the cursor has no new rows.

A one-row request is valid for a single lookup, but cannot advance an inclusive
paging loop once the cursor matches a row. Counts outside 1–10,000 return
`INVALID_ARGUMENT` over gRPC; the server does not silently truncate them.

Ascending reads return a stable committed prefix. Descending latest lookups do
not apply PostgreSQL's ascending visibility barrier. Each page has its own read
snapshot; multiple pages are not a fixed snapshot of the whole log. Keep consumers
idempotent and obtain fresh CCC observations after a long replay.

## Subscriptions

`CatchUpSubscribeToEvents` takes an `after_position`. An explicit position is exclusive. If omitted, the latest stored matching event is delivered once, followed by newer matches. Every subscription event is read from the backend in position order; notifications and subscription-owned idle watchdog hints trigger those reads. A projector should persist the position of the last event it durably processed and resume from that position on restart. See [Delivery Guarantees](./delivery-guarantees).
