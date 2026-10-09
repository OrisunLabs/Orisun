---
id: overview
title: What is Orisun?
description: What Orisun is, the guarantees it makes, and where to start.
slug: /
---

Orisun is an open-source event database for decisions that must stay correct as facts change. It preserves complete event history and lets applications declare the events a command depends on. Orisun commits the resulting events only if that declared context is still current, then delivers matching committed events sequentially within each subscription.

These pages describe the breaking changes after `0.13.0`: notifications use
Core NATS hints, FoundationDB is removed, and storage upgrades automatically
from `0.13.0`; older storage formats are rejected. If you are reading **Next**,
these changes are unreleased. Select **0.13.0** in the version menu when running
that preceding release.

The mechanism behind that promise is **Command Context Consistency**: commands query the exact events they depend on, and writes succeed only if that context has not changed.

It stores the event log transactionally in PostgreSQL or SQLite, and delivers committed events from backend reads with Core NATS wake-ups, including
catch-up replay and live subscriptions. Storage, consistency checks,
notification relays, indexes, auth, and gRPC APIs ship as one deployable server.

## Guarantees

- **Decisions scoped to real context.** A write carries every query-level observation the command depended on and commits only if all of those contexts are unchanged. You do not need to force every invariant into a single stream.
- **Content-scoped consistency checks.** Carry query-level observations into `SaveEventsV2` and save only while every context the command read is current.
- **Replay from durable progress.** Each subscription reads matching events from its backend cursor. Missed hints are recovered through a subscription-owned idle watchdog while NATS is healthy. Consumers persist their own checkpoints and handle at-least-once replay.
- **Ordered subscriptions.** Each subscription delivers matching events in ascending boundary position order.
- **Runtime boundary management.** New and imported physical boundaries are
  durable lifecycle events, provisioned without restarting the server or
  maintaining a startup boundary list.
- **Same API on every backend.** SQLite and PostgreSQL expose the
  identical gRPC surface, so deployments can grow without client changes.

## How it works

1. **Store** events transactionally in the selected backend.
2. **Check** command consistency by querying the event subset the command depends on.
3. **Notify** subscriptions with empty, transient Core NATS boundary hints.
4. **Deliver** matching events through ordered backend reads, using subscription cursors and NATS idle watchdog hints for recovery.

## Quick start

SQLite is the fastest local loop. Event log, admin state, indexes, projector checkpoints, and embedded JetStream run from one binary with no separate database.

1. Download `orisun-sqlite` from [GitHub Releases](https://github.com/OrisunLabs/Orisun/releases).
2. Start it with the [SQLite binary example](getting-started.md#run-sqlite-from-a-binary).
3. Verify gRPC with [Verify the API](getting-started.md#verify-the-api).
4. Define the application log with
   [Create the application boundary](getting-started.md#create-the-application-boundary)
   and wait for it to become active.
5. Save an event with [Save your first event](getting-started.md#save-your-first-event).

Move to PostgreSQL when you need multiple Orisun nodes or
database-managed operations. SQLite is single-node only and requires NATS
clustering disabled.

## Pick your path

| Goal | Read first | Then read |
| --- | --- | --- |
| Try Orisun locally | [Getting Started](getting-started.md) | [Tutorial](tutorial.md) |
| Embed Orisun in a Go service | [Go Embedding](embedding/go.md) | [Storage Backends](concepts/storage-backends.md) |
| Model a business invariant | [Command Context Consistency](concepts/command-context-consistency.md) | [Positions](concepts/positions.md) |
| Save, query, and subscribe | [EventStore API](api/eventstore.md) | [Clients](api/clients.md) |
| Upgrade existing storage | [Storage upgrade policy](operations/upgrading-event-envelope.md) | [Deployment](operations/deployment.md#notification-transport) |
| Create or import boundaries | [Admin API](api/admin.md#boundary-lifecycle) | [Boundary configuration](operations/configuration.md#boundary-management) |
| Prepare production settings | [Configuration](operations/configuration.md) | [Deployment](operations/deployment.md) |
| Debug a running node | [Troubleshooting](operations/troubleshooting.md) | [Observability](operations/observability.md) |

## When Orisun fits

Use Orisun when:

- commands need to read event history before deciding what to write,
- stale context would make an otherwise valid command unsafe,
- consistency depends on a subset of events, not always a fixed stream,
- projectors need to recover from downtime without relying only on broker retention,
- you want the event store, subscriptions, gRPC API, auth, indexes, and telemetry in one server.

Choose another tool when you only need transient messaging or a general-purpose queue with no durable event-log semantics. See [Comparing Orisun](comparison.md) for how Orisun differs from Kafka, EventStoreDB, PostgreSQL `LISTEN/NOTIFY`, and NATS JetStream.
