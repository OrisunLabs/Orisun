---
title: Indexing
description: Create JSON indexes for criteria queries and CCC checks.
---

Criteria queries match JSON payload fields. Without indexes, PostgreSQL and SQLite reads and CCC checks may scan the full boundary event table. FoundationDB requires either a native position range (anchored by `__commitPosition` or `__writeId`) or a ready covering secondary index. Other uncovered criteria return `FAILED_PRECONDITION`.

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

### High-throughput CCC

All PostgreSQL saves use one group-commit implementation, including batches of
one and unconditional writes. It resolves CCC event batches as criterion
state rather than issuing one database query per request. It:

- deduplicates the batch's AND criteria
- reads each distinct criterion's latest persisted position using literal predicates and `ORDER BY ... LIMIT 1` branches in one statement
- groups criteria by key shape and matches final event documents through direct criterion-ID lookups
- evaluates each observation's OR query in request order, updating criterion state after
  every accepted request
- bulk-inserts accepted events once

Literal predicates let PostgreSQL use event-type partial indexes during write-time
CCC checks, just as it does during context reads. Historical matches are no longer
joined and ranked for each criterion shape. Applications still own their indexes;
unindexed criteria can require scans.

This supports duplicate contexts, different keys, multi-tag AND criteria,
multi-criterion OR queries, multiple query-level observations, and query-less
events that affect later queried saves. An earlier event in the transaction
still invalidates a later observation exactly as it would if the saves
committed separately.

Every event in a multi-event save receives a consecutive global ID and shares
the save's transaction ID. For each criterion, PostgreSQL's in-batch state
points to the highest event in that save that matched the criterion. Later
queued saves therefore observe the same `(transaction_id, global_id)` position
that they would observe after a separately committed multi-event save.

SQLite uses queue-ordered checks with request-local savepoints for every save
inside the shared group-commit transaction. Each request inserts its events in
chunks while preserving atomic rollback of its events, write context, and
positions. This retains the transaction and fsync savings of group commit.
CCC checks remove redundant OR criteria, then find the latest match for each
remaining AND criterion separately and compare the maximum full position.
This lets each criterion use its own matching index without sorting all events
matching the combined OR query. Index each remaining criterion shape; an
unindexed branch can still scan history even when another branch is indexed.

FoundationDB needs a ready covering index for each criterion that does not
select a native range through `__commitPosition` or `__writeId`. A V2 request with several observations can therefore depend on
several indexes; create and wait for all of them before enabling that command
path.

Create indexes for every criterion shape used by high-volume command paths. A
simple `customer_id` criterion needs the simple index above; a criterion on
`customer_id AND region` should have a composite index with both fields. On
PostgreSQL and SQLite, unindexed criteria remain correct but can scan the
boundary event table. FoundationDB rejects a criteria read or CCC observation with
`FAILED_PRECONDITION` if it has neither a native position range nor a ready
covering secondary index.

PostgreSQL group commit bulk-inserts accepted multi-event saves. For
burst-oriented workloads, start performance testing with
`ORISUN_PG_GC_MAX_BATCH_REQUESTS=512`,
`ORISUN_PG_GC_MAX_BATCH_EVENTS=1024`, and a small coalescing window such as
`ORISUN_PG_GC_MAX_DELAY=1ms`. The delay trades up to one millisecond of
low-volume latency for fuller batches, so measure it under the target command
mix. PostgreSQL rejects malformed requests before SQL batching; SQLite uses
request-local validation and savepoints.

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

`value_type` controls the index expression, not CCC equality. PostgreSQL and
SQLite equality predicates compare scalar values as text regardless of index definitions:
JSON number `42` and string `"42"` match the criterion `"42"`, while string
`"042"` does not. Creating or dropping an index must not change those matches.
Use `TEXT` indexes for these equality queries, including keys whose JSON values
are numbers or booleans. Typed index expressions are not matching search keys
for the scalar-text CCC predicates.

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
    {"key": "__eventType", "operator": "=", "value": "OrderPlaced"}
  ],
  "condition_combinator": "AND"
}
EOF
```

Each condition `operator` must be one of `=`, `>`, `<`, `>=`, or `<=`; any other value is rejected. `condition_combinator` is `AND` by default, or `OR` when any condition may match.

These are index-definition conditions. Query tag operators use `eq`, `ne`,
`gt`, `gte`, `lt`, and `lte`; see [Tag comparison operators](../api/eventstore#tag-comparison-operators).

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
`CreateIndex` to adopt it into the inventory. Adoption succeeds only when the
physical index matches the requested definition. A conflicting physical index
is rejected without overwriting its metadata.

## Backend Behavior

PostgreSQL uses concurrent JSONB expression-index builds so boundary writes can
continue during creation. Orisun verifies `pg_index.indisvalid` before reporting
an index as `READY`. If a concurrent build fails or a retry finds an invalid
physical index, Orisun drops that invalid index and leaves the logical
definition `BUILDING` so the operation can be retried cleanly. PostgreSQL and
SQLite append `transaction_id DESC, global_id DESC` after the declared fields
in API-managed indexes. SQLite uses JSON expression indexes. An equality lookup
on the full declared `TEXT` field shape can
therefore find its latest matching event without sorting the context's complete
history.

Fresh stores create indexes in the current format. Startup rejects older storage
versions instead of rebuilding historical index definitions. Index creation and
retry are owned by the index API lifecycle.


## Naming and safety

Index names are boundary-local logical names. Orisun validates names before creating backend objects.

On PostgreSQL and SQLite, `CreateIndex` is idempotent for an existing matching
definition. Reusing its name with different fields, field types, conditions, or
condition combinator returns `ALREADY_EXISTS`; it does not replace the index or
rewrite its metadata. Use `DropIndex` before creating a replacement definition.
PostgreSQL also rejects names whose full physical form
`<boundary>_<name>_idx` exceeds 63 bytes, preventing identifier truncation from
aliasing another index.

Use migrations or a controlled startup task for production index creation. Creating indexes during high-traffic command paths can add avoidable latency.
