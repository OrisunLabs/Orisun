---
id: comparison
title: Comparing Orisun
description: How Orisun differs from Kafka, EventStoreDB, PostgreSQL LISTEN/NOTIFY, and NATS JetStream, and when to choose each.
slug: /comparison
---

Orisun sits at a specific point on a tradeoff curve: an event database for decisions that must stay correct as facts change, where the durable log, content-scoped consistency, and live delivery ship in one server. This page is for evaluators deciding between Orisun and adjacent tools. It is not a claim that Orisun is universally better; every tool below is the right choice for some workload.

## What makes Orisun different

Two properties define Orisun's position:

1. **Consistency is scoped by event content, not by a fixed stream.** A command declares the event subset it depends on with JSON criteria, then saves only if that subset is still at the expected position. This is Orisun's [Command Context Consistency](./concepts/command-context-consistency) model.
2. **The event store and the delivery layer are one system.** The durable log in PostgreSQL, YugabyteDB, SQLite, or FoundationDB is the source of truth; embedded NATS JetStream is the live-delivery buffer; durable publisher checkpoints guarantee no committed event is skipped. See [Delivery Guarantees](./concepts/delivery-guarantees).

Most adjacent tools optimize one of consistency, throughput, simplicity, or decoupling and trade the rest. Orisun optimizes for the closed loop: read the relevant event history, make a decision, commit only if that context is still valid, then deliver the committed result from one deployable server.

## At a glance

| Tool | Consistency model | Source of truth | Delivery | Ops model |
| --- | --- | --- | --- | --- |
| Orisun | Content-scoped optimistic (CCC) | PostgreSQL, YugabyteDB, SQLite, or FoundationDB | Catch-up replay + live JetStream, per-boundary order, no skips | One deployable server |
| Kafka | Partition order; no command consistency check | Kafka log (retention-bounded) | Log tailing by offset | Broker cluster (KRaft or ZooKeeper) |
| EventStoreDB | Stream / aggregate optimistic (expected revision) | EventStoreDB store | Subscriptions + projections | Dedicated server |
| PostgreSQL `LISTEN/NOTIFY` | None | Your tables | Best-effort notifications (≤ 8 KB, not durable, no replay) | Existing Postgres |
| NATS JetStream | None | JetStream streams | Durable streams, acks, retention | NATS server cluster |

## Orisun vs Kafka

[Kafka](https://kafka.apache.org) is a high-throughput distributed log. It excels as a decoupled event backbone across many producers and consumers, replay pipelines, and very high ingest rates. It has no notion of a command consistency check. Ordering is per partition, consumers track offsets, and events are retained for a configurable window rather than forever.

Choose Kafka when you need a general-purpose streaming backbone across many services, maximum ingest throughput, and producers/consumers that are fully decoupled.

Choose Orisun when commands must read event history, make a consistency-checked decision, and commit and publish in one ordered loop, without assembling a broker, publisher, and store yourself. Kafka and Orisun are not mutually exclusive: Orisun can front Kafka-free delivery with embedded JetStream, or sit beside an existing Kafka deployment.

## Orisun vs EventStoreDB

[EventStoreDB](https://www.eventstore.com/) is a mature, purpose-built event store. Its consistency model is stream- and aggregate-centric: you write to a named stream with an expected revision, and the server rejects concurrent writes to that stream. Projections and subscriptions read from streams.

The core difference is the consistency boundary. EventStoreDB asks you to choose the aggregate stream up front and scope consistency to it. Orisun lets each command define its consistency context dynamically by querying event content; a transfer can depend on two accounts, or a limit check on a customer, product, and risk class, without pre-declaring a stream.

Choose EventStoreDB when your domain maps cleanly to aggregate streams and you want a mature, stream-focused tool with a broad ecosystem.

Choose Orisun when consistency spans a subset of events that does not map to one fixed stream, or when you want the store, delivery, auth, indexes, and API in one server.

## Orisun vs PostgreSQL LISTEN/NOTIFY

`LISTEN/NOTIFY` is PostgreSQL's built-in notification channel. A session subscribes to a channel and receives notifications when `NOTIFY` fires. It is a wake-up signal, not an event log: a notification fired with no listener is lost, the payload is capped at 8 KB, and there is no replay or cross-restart ordering guarantee.

Choose `LISTEN/NOTIFY` when you only need to tell listeners that data in tables you already own has changed, and durability, ordering, and replay are not required.

Choose Orisun when you need a durable, ordered, replayable event log with consistency checks rather than a signal. Orisun itself uses `LISTEN/NOTIFY` internally as a publisher wake-up, but correctness comes from the persisted log and durable checkpoints, never from the signal. See [Delivery Guarantees](./concepts/delivery-guarantees#notifications-are-not-the-guarantee).

## Orisun vs NATS JetStream

[JetStream](https://docs.nats.io/nats-concepts/jetstream) is NATS's durable streaming layer: persistent subjects, retention policies, consumer acknowledgements, and work queues. It is an excellent delivery layer, which is why Orisun embeds it for live delivery. JetStream alone is not an event store: it has no content-based consistency check or command-context optimistic locking.

Choose JetStream directly when you need durable messaging and streaming across services, and the consistency model lives in your application or another system.

Choose Orisun when you want JetStream-class delivery plus an event store and a consistency model in one server, instead of running them as separate systems. Orisun can also connect to an external JetStream-enabled NATS server with `ORISUN_NATS_URL`.

## When to choose Orisun

Choose Orisun when:

- commands read event history and commit conditionally on it,
- consistency spans a subset of events rather than always a fixed stream,
- you want the store, publisher, API, auth, indexes, and telemetry in one deployable server,
- projectors must recover from downtime without depending only on broker retention.

Reach for another tool when:

- you only need transient messaging or a general-purpose queue, such as Kafka, NATS, or RabbitMQ,
- your domain maps cleanly to aggregate streams and you want a mature stream store, such as EventStoreDB,
- you already own the data model in PostgreSQL and only need a change signal via `LISTEN/NOTIFY`,
- you need a massively sharded, cross-organization streaming backbone at very high throughput, such as Kafka.

If Orisun fits, start with [Getting Started](./getting-started) and the [Tutorial](./tutorial).
