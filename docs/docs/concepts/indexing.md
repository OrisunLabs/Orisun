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

PostgreSQL uses JSONB expression indexes. SQLite uses JSON expression indexes.

## Naming and safety

Index names are boundary-local logical names. Orisun validates names before creating backend objects.

Use migrations or a controlled startup task for production index creation. Creating indexes during high-traffic command paths can add avoidable latency.
