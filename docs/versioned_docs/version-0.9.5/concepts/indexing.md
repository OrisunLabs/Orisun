---
title: Indexing
description: Create JSON indexes for criteria queries and CCC checks.
---

Criteria queries match JSON payload fields. Without indexes, PostgreSQL and SQLite reads and CCC checks may scan the full boundary event table. FoundationDB is stricter: criteria reads and CCC checks require a ready covering index and return `FAILED_PRECONDITION` when no such index exists.

Create indexes for fields used in:

- command context criteria
- projector catch-up filters
- common read models
- high-volume event categories

## Index API

Index management is exposed on the EventStore gRPC service, not the Admin service. This matters for embedded deployments: applications can manage indexes without exposing Admin.

All examples assume:

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
```

## Simple Index

```bash
grpcurl -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id","fields":[{"json_key":"customer_id","value_type":"TEXT"}]}' \
  localhost:5005 orisun.EventStore/CreateIndex
```

### High-throughput PostgreSQL CCC

PostgreSQL group commit resolves canonical CCC event batches as criterion state
rather than issuing one database query per request. It:

- deduplicates the batch's AND criteria
- groups criteria by their indexed key shape
- reads the current position of each criterion set-wise
- computes which incoming events match every criterion
- evaluates AND/OR queries in request order, updating criterion state after
  every accepted request
- bulk-inserts accepted events once

This supports duplicate contexts, different keys, multi-tag AND criteria,
multi-criterion OR queries, and query-less events that affect later queried
saves. An earlier event in the transaction still invalidates a later query
exactly as it would if the saves committed separately.

Every event in a multi-event save receives a consecutive global ID and shares
the save's transaction ID. For each criterion, in-batch state points to the
highest event in that save that matched the criterion. Later queued saves
therefore observe the same `(transaction_id, global_id)` position that they
would observe after a separately committed multi-event save.

Create indexes for every criterion shape used by high-volume command paths. A
simple `customer_id` criterion needs the simple index above; a criterion on
`customer_id AND region` should have a composite index with both fields.
Unindexed criteria remain correct but can scan the boundary event table.

The specialized path for independent single-tag contexts also bulk-inserts
multi-event saves. For burst-oriented workloads, start performance testing with
`ORISUN_PG_GC_MAX_BATCH_REQUESTS=512`,
`ORISUN_PG_GC_MAX_BATCH_EVENTS=1024`, and a small coalescing window such as
`ORISUN_PG_GC_MAX_DELAY=1ms`. The delay trades up to one millisecond of
low-volume latency for fuller batches, so measure it under the target command
mix. Request-local validation failures use the isolated ordered path.
YugabyteDB currently uses its ordered criterion-lock path.

## Composite Index

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "orders",
  "name": "category_priority",
  "fields": [
    {"json_key": "category", "value_type": "TEXT"},
    {"json_key": "priority", "value_type": "TEXT"}
  ]
}
EOF
```

## Field value types

`value_type` controls how Orisun casts the JSON key in the index expression. Queries that compare the same key use the matching cast.

| Value | Backend cast |
| --- | --- |
| `TEXT` | Text (default). |
| `NUMERIC` | Numeric, for range and ordering predicates. |
| `BOOLEAN` | Boolean. |
| `TIMESTAMPTZ` | Timestamp with time zone. |

## Partial Index

A partial index covers only events that match its `conditions`, keeping the index small and focused on one event category.

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "orders",
  "name": "placed_amount",
  "fields": [
    {"json_key": "amount", "value_type": "NUMERIC"}
  ],
  "conditions": [
    {"key": "eventType", "operator": "=", "value": "OrderPlaced"}
  ],
  "condition_combinator": "AND"
}
EOF
```

Each condition `operator` must be one of `=`, `>`, `<`, `>=`, or `<=`; any other value is rejected. `condition_combinator` is `AND` by default, or `OR` when any condition may match.

## Drop An Index

```bash
grpcurl -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/DropIndex
```

## Inspect Indexes

Use `ListIndexes` for a boundary-wide inventory and `GetIndex` for one logical
name:

```bash
grpcurl -H "$AUTH" -d '{"boundary":"orders"}' \
  localhost:5005 orisun.EventStore/ListIndexes

grpcurl -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/GetIndex
```

Each definition includes its fields, conditions, combinator, and state.
`BUILDING` means the index is registered but its backfill has not completed;
`READY` means it can be used. FoundationDB exposes its live backfill state.
Synchronous PostgreSQL and SQLite creation normally returns only after the
index is ready.

The inventory contains indexes managed through Orisun's index API. It does not
attempt to parse arbitrary database-native indexes. After upgrading an existing
PostgreSQL installation, recreate an existing logical definition with
`CreateIndex` to adopt it into the inventory; the physical `IF NOT EXISTS`
creation remains idempotent.

## Backend Behavior

PostgreSQL uses concurrent JSONB expression-index builds so boundary writes can
continue during creation. Orisun verifies `pg_index.indisvalid` before reporting
an index as `READY`. If a concurrent build fails or a retry finds an invalid
physical index, Orisun drops that invalid index and leaves the logical
definition `BUILDING` so the operation can be retried cleanly. SQLite uses JSON
expression indexes.

## Naming and safety

Index names are boundary-local logical names. Orisun validates names before creating backend objects.

Use migrations or a controlled startup task for production index creation. Creating indexes during high-traffic command paths can add avoidable latency.
