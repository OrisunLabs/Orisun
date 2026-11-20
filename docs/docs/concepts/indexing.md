---
title: Indexing
description: Create JSON indexes for criteria queries and CCC checks.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Criteria queries match JSON payload fields. Without indexes, PostgreSQL and SQLite reads and CCC checks may scan the full boundary event table.

Create indexes for fields used in:

- command context criteria
- projector catch-up filters
- common read models
- high-volume event categories

## Index API

Index management is exposed on the EventStore gRPC service, not the Admin service. This matters for embedded deployments: applications can manage indexes without exposing Admin.

Examples assume an authenticated client from
[Connect and authenticate](../api/eventstore#connect-and-authenticate). The Go
examples import `github.com/oexza/orisun-client-go/eventstore`; the Node.js
examples import `ValueType` and `ConditionCombinator` from
`@orisun/eventstore-client`; the Java examples import
`com.orisun.eventstore.Eventstore`. The grpcurl examples assume:

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
```

## Simple Index

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "orders",
	Name:     "customer_id",
	Fields: []*eventstore.IndexField{
		{JsonKey: "customer_id", ValueType: eventstore.ValueType_TEXT},
	},
})
if err != nil {
	log.Fatal(err)
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.createIndex({
  boundary: 'orders',
  name: 'customer_id',
  fields: [
    { jsonKey: 'customer_id', valueType: ValueType.TEXT },
  ],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.createIndex(Eventstore.CreateIndexRequest.newBuilder()
    .setBoundary("orders")
    .setName("customer_id")
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("customer_id")
        .setValueType(Eventstore.ValueType.TEXT))
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -plaintext -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "orders",
  "name": "customer_id",
  "fields": [
    {"json_key": "customer_id", "value_type": "TEXT"}
  ]
}
EOF
```

  </TabItem>
</Tabs>

## Composite Index

List fields in the order criteria use them. A criterion on `category AND
priority` needs this composite index; a `category`-only index does not cover
the second key.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "orders",
	Name:     "category_priority",
	Fields: []*eventstore.IndexField{
		{JsonKey: "category", ValueType: eventstore.ValueType_TEXT},
		{JsonKey: "priority", ValueType: eventstore.ValueType_TEXT},
	},
})
if err != nil {
	log.Fatal(err)
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.createIndex({
  boundary: 'orders',
  name: 'category_priority',
  fields: [
    { jsonKey: 'category', valueType: ValueType.TEXT },
    { jsonKey: 'priority', valueType: ValueType.TEXT },
  ],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.createIndex(Eventstore.CreateIndexRequest.newBuilder()
    .setBoundary("orders")
    .setName("category_priority")
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("category")
        .setValueType(Eventstore.ValueType.TEXT))
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("priority")
        .setValueType(Eventstore.ValueType.TEXT))
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -plaintext -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
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

  </TabItem>
</Tabs>

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
| `NUMERIC` | Backend numeric cast; not the exact-decimal query comparison expression. |
| `BOOLEAN` | PostgreSQL boolean cast; SQLite integer cast. |
| `TIMESTAMPTZ` | SQLite text expression. PostgreSQL emits a text-to-timestamptz cast, which PostgreSQL rejects in an index because it is not immutable. |

Typed index definitions do not change query semantics. Numeric range queries
use exact-decimal comparison functions, so a `NUMERIC` cast index does not
directly accelerate them. For current content queries, start with `TEXT`
indexes on selective equality keys. PostgreSQL partial-index conditions compare
text; SQLite conditions use the indexed field type when one is declared. Keep
partial-index predicates aligned with the backend query expression.

## Partial Index

A partial index covers only events that match its `conditions`, keeping the
index small and focused on one event category. This index covers
`customer_id` lookups on `OrderPlaced` events only. `__eventType` is the
event type Orisun stores in every event's `data`.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "orders",
	Name:     "placed_customer",
	Fields: []*eventstore.IndexField{
		{JsonKey: "customer_id", ValueType: eventstore.ValueType_TEXT},
	},
	Conditions: []*eventstore.IndexCondition{
		{Key: "__eventType", Operator: "=", Value: "OrderPlaced"},
	},
	ConditionCombinator: eventstore.ConditionCombinator_AND,
})
if err != nil {
	log.Fatal(err)
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.createIndex({
  boundary: 'orders',
  name: 'placed_customer',
  fields: [
    { jsonKey: 'customer_id', valueType: ValueType.TEXT },
  ],
  conditions: [
    { key: '__eventType', operator: '=', value: 'OrderPlaced' },
  ],
  conditionCombinator: ConditionCombinator.AND,
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.createIndex(Eventstore.CreateIndexRequest.newBuilder()
    .setBoundary("orders")
    .setName("placed_customer")
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("customer_id")
        .setValueType(Eventstore.ValueType.TEXT))
    .addConditions(Eventstore.IndexCondition.newBuilder()
        .setKey("__eventType")
        .setOperator("=")
        .setValue("OrderPlaced"))
    .setConditionCombinator(Eventstore.ConditionCombinator.AND)
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -plaintext -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "orders",
  "name": "placed_customer",
  "fields": [
    {"json_key": "customer_id", "value_type": "TEXT"}
  ],
  "conditions": [
    {"key": "__eventType", "operator": "=", "value": "OrderPlaced"}
  ],
  "condition_combinator": "AND"
}
EOF
```

  </TabItem>
</Tabs>

Each condition `operator` must be one of `=`, `>`, `<`, `>=`, or `<=`; any other value is rejected. `condition_combinator` is `AND` by default, or `OR` when any condition may match.

These are index-definition conditions. Query tag operators use `eq`, `ne`,
`gt`, `gte`, `lt`, and `lte`; see [Tag comparison operators](../api/eventstore#tag-comparison-operators).

## Drop An Index

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.DropIndex(ctx, &eventstore.DropIndexRequest{
	Boundary: "orders",
	Name:     "customer_id",
})
if err != nil {
	log.Fatal(err)
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.dropIndex({
  boundary: 'orders',
  name: 'customer_id',
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.dropIndex(Eventstore.DropIndexRequest.newBuilder()
    .setBoundary("orders")
    .setName("customer_id")
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -plaintext -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/DropIndex
```

  </TabItem>
</Tabs>

## Inspect Indexes

Use `ListIndexes` for a boundary-wide inventory and `GetIndex` for one logical
name:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
list, err := client.ListIndexes(ctx, "orders")
if err != nil {
	log.Fatal(err)
}
for _, idx := range list.Indexes {
	log.Printf("%s: %s", idx.Name, idx.State)
}

one, err := client.GetIndex(ctx, "orders", "customer_id")
if err != nil {
	log.Fatal(err) // codes.NotFound when the name is not registered
}
log.Printf("%s: %s", one.Index.Name, one.Index.State)
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const list = await client.listIndexes('orders');
for (const idx of list.indexes) {
  console.log(idx.name, idx.state);
}

const one = await client.getIndex('orders', 'customer_id');
console.log(one.index.name, one.index.state);
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.ListIndexesResponse list = client.listIndexes("orders");
for (Eventstore.IndexDefinition idx : list.getIndexesList()) {
    System.out.println(idx.getName() + ": " + idx.getState());
}

Eventstore.GetIndexResponse one = client.getIndex("orders", "customer_id");
System.out.println(one.getIndex().getName() + ": " + one.getIndex().getState());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -plaintext -H "$AUTH" -d '{"boundary":"orders"}' \
  localhost:5005 orisun.EventStore/ListIndexes

grpcurl -plaintext -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/GetIndex
```

  </TabItem>
</Tabs>

Each definition includes its fields, conditions, combinator, and state.
`BUILDING` means the definition is registered but its physical build has not completed;
`READY` means it can be used.
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

Fresh stores create indexes in the current format. Storage from `0.13.0` upgrades without rebuilding indexes; older formats are rejected. Index creation and
retry are owned by the index API lifecycle.

## Indexes and High-Throughput CCC

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

Equality criteria use the direct criterion-ID lookup. Criteria with non-equality
operators use generated predicates against each accepted event document so
in-batch checks retain the same semantics as persisted reads.

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

Create indexes for every criterion shape used by high-volume command paths. A
simple `customer_id` criterion needs the simple index above; a criterion on
`customer_id AND region` should have a composite index with both fields. On
PostgreSQL and SQLite, unindexed criteria remain correct but can scan the
boundary event table.

PostgreSQL group commit bulk-inserts accepted multi-event saves. For
burst-oriented workloads, start performance testing with
`ORISUN_PG_GC_MAX_BATCH_REQUESTS=512`,
`ORISUN_PG_GC_MAX_BATCH_EVENTS=1024`, and a small coalescing window such as
`ORISUN_PG_GC_MAX_DELAY=1ms`. The delay trades up to one millisecond of
low-volume latency for fuller batches, so measure it under the target command
mix. PostgreSQL rejects malformed requests before SQL batching; SQLite uses
request-local validation and savepoints.

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
