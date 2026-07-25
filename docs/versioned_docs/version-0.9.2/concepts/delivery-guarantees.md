---
title: Delivery Guarantees
description: Understand publishing order, catch-up, and duplicate delivery.
---

PostgreSQL, YugabyteDB, SQLite, or FoundationDB is the durable source of truth. Embedded NATS JetStream is the real-time delivery layer.

Wake-up signals are hints. Correctness comes from durable checkpoints and ordered catch-up reads.

## Per-Boundary Guarantees

| Guarantee | How Orisun enforces it |
| --- | --- |
| No skipped committed events | After each fully acknowledged batch, the publisher stores its final position in the selected backend and resumes from the next position. |
| Sequential publishing | The publisher drains events in ascending event-log position and rejects non-advancing batches. |
| Stable committed prefix | PostgreSQL avoids exposing in-flight transactions; SQLite serializes commits through one writer per boundary; FoundationDB reads from ordered committed versionstamps. |
| Single active publisher | PostgreSQL nodes acquire a boundary lock; FoundationDB nodes acquire a token-fenced FDB lease lock; SQLite runs exactly one active node. |

## Notifications are not the guarantee

PostgreSQL `LISTEN/NOTIFY`, SQLite wake-ups, FoundationDB watch signals, and polling only tell the publisher that there may be work to do. If a signal is missed, periodic polling still drains the committed event log from the persisted checkpoint.

This design is why no committed event is skipped even when a wake-up signal is delayed or lost.

## At-least-once delivery

Publishing is at least once at the boundary between publishing a batch to NATS and updating its final checkpoint.

If NATS accepts part or all of a batch and the checkpoint is not persisted, events from that batch can be republished. Consumers should deduplicate by:

- `event_id`
- NATS message ID
- idempotent projector writes

## Ordering Scope

Sequential publishing is per boundary. Different boundaries can publish independently.

Within a boundary, consumers should process positions monotonically and persist their own projector checkpoint after side effects are durable.

## JetStream retention is in memory

The embedded JetStream stream uses memory storage. It is the live-delivery buffer, not the source of truth. The durable event log in PostgreSQL, YugabyteDB, SQLite, or FoundationDB is the source of truth. Two consequences follow:

- Live retention is bounded by `ORISUN_NATS_EVENT_STREAM_MAX_BYTES` (default 512 MB), `ORISUN_NATS_EVENT_STREAM_MAX_MSGS`, and `ORISUN_NATS_EVENT_STREAM_MAX_AGE` (default 5m), per boundary. Events older than the window or beyond the size cap are dropped from the live buffer.
- A subscriber that falls behind the retention window does not lose events. It falls out of live delivery and is served from the durable store by the catch-up phase instead.

The age window must comfortably exceed the catch-up handover grace (about 10 seconds) so a subscriber transitioning from catch-up to live does not land in a gap. Keep `ORISUN_NATS_EVENT_STREAM_MAX_AGE` well above that grace.

Because the stream is in memory, JetStream state does not survive a restart. After a restart, subscribers re-establish from their persisted position through catch-up. Size the retention window for your slowest expected subscriber, not for durability; durability already lives in the backend.

## Subscriber catch-up

`CatchUpSubscribeToEvents` has two phases:

1. Read committed events after the requested position from the durable store.
2. Subscribe to JetStream for live delivery.

The catch-up phase is what lets a subscriber recover from downtime without depending on JetStream retention alone.
