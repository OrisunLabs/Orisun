---
title: Delivery Guarantees
description: Understand ordered backend delivery, notification recovery, and replay.
---

PostgreSQL or SQLite is the durable source of truth. Every
subscription event comes from a backend read. Core NATS carries
empty boundary wake-up hints; it carries no application event data or metadata.

## Per-Boundary Guarantees

| Guarantee | How Orisun enforces it |
| --- | --- |
| Stable committed prefix | PostgreSQL preserves its ascending visibility barrier; SQLite serializes commits through one writer per boundary. |
| Recovery from missed hints | Each subscription publishes a NATS hint after its idle threshold; receiving that hint resumes backend reads after the last successfully delivered position. Healthy NATS is required. |
| Ordered delivery | One reader per subscription delivers matching events in ascending `(commit_position, prepare_position)` order and validates each complete batch before delivery. |
| Bounded memory | One pending wake-up and one bounded backend batch per subscription; handlers run sequentially and provide backpressure. |
| Relay ownership | A boundary lease coordinates signal forwarding. Duplicate hints are harmless and never advance an event cursor. |

## Notifications are not the guarantee

PostgreSQL `LISTEN/NOTIFY`, SQLite post-commit wake-ups,
tell a relay that a boundary may have new events. The relay forwards
an empty hint on `ORISUN_NOTIFICATIONS___<boundary>.changed.v1`.

Subscriptions establish a Core NATS listener and flush its registration before
their initial read. Failed registration is retried with bounded backoff. NATS
automatically restores listeners after reconnecting. Startup and reconnect each
publish an empty boundary hint; its receipt triggers the backend drain. Hints require no acknowledgements and carry no delivery progress.

`ORISUN_SUBSCRIPTION_IDLE_THRESHOLD` defaults to `10s` and must be positive.
Each subscription tracks its last received hint, even when its query matches no
events. Once that silence reaches the threshold, its watchdog publishes an empty
boundary hint through NATS. Receiving the hint triggers the normal backend drain;
publishing it does not directly read storage. Receipt resets the idle deadline.

There is no periodic backend polling. Healthy embedded NATS is a requirement for
live subscription delivery and missed-hint recovery. While NATS is unavailable,
new delivery waits for listener recovery. Initial catch-up also waits for receipt of the startup ping.
Backend query latency and handler speed also affect delivery latency.

## At-least-once delivery

The cursor advances after a handler returns successfully. Failed delivery leaves
the event eligible for replay. A successful gRPC send is not acknowledgement of
an application's transaction or side effects.

Consumers must persist their own checkpoint after durable side effects and
resume with that position after reconnecting. A crash between delivery and that
checkpoint can replay events. Deduplicate by `event_id` or use idempotent
projector writes; notification message IDs are not event identities.

## Ordering Scope

Ordering is per subscription within a boundary. Different boundaries and
subscriptions progress independently. The backend evaluates the complete query
for both historical and newly committed events; subscriptions do not evaluate
criteria against payloads locally.

## Transient notification transport

Core NATS retains no notification history and creates no per-boundary streams
or consumers. Listeners use the NATS client’s default bounded pending queue. Their callbacks
record one coalesced pending wake-up in the drain channel. Lost or dropped hints
are recovered through idle watchdog hints while NATS is healthy. JetStream remains in
use for PostgreSQL/SQLite leases and admin messaging.

Notification outages and server restarts do not supply delivery progress.
The old event-stream retention and polling-publisher batch settings have been
removed. Configure the notification idle watchdog with
`ORISUN_SUBSCRIPTION_IDLE_THRESHOLD`.

## Subscriber catch-up

`CatchUpSubscribeToEvents` uses one backend drain throughout its lifetime.
An explicit `after_position` is exclusive. When omitted, the latest stored
matching event is delivered once, followed by newer matches in ascending order.
If no initial match exists, subsequent drains read future matches ascending.

For filtered subscriptions, only the last successfully delivered matching event
becomes the cursor. Empty reads and nonmatching events do not advance it.
Publisher checkpoint APIs and provisioning have been removed. Application and
projector checkpoints remain required.

## Breaking release deployment

Stop older server nodes before deploying this release. Notification subjects
carry empty Core NATS hints; event streams and publisher checkpoints are no
longer provisioned or consumed. Existing storage must use the supported format.
See the [deployment procedure](../operations/deployment#notification-transport)
and [storage upgrade policy](../operations/upgrading-event-envelope).
