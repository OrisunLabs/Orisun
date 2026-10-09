---
title: EventStore API
description: Save, query, subscribe, inspect the server, and manage indexes.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The EventStore service owns event operations:

- `SaveEventsV2`
- `GetEvents`
- `GetWriteContext`
- `GetLatestByCriteria`
- `CatchUpSubscribeToEvents`
- `Ping`
- `GetServerInfo`
- `CreateIndex`
- `DropIndex`
- `ListIndexes`
- `GetIndex`

## Connect and authenticate

Every example on this page assumes an authenticated client connected to a running server. The default credentials are `admin:changeit`; change `ORISUN_ADMIN_PASSWORD` before exposing the server.

Pick your client once; every tabbed example below follows that choice across this page and the tutorial.

:::important
The `orders` and `ledger` boundaries used below must already exist and report
`BOUNDARY_LIFECYCLE_STATUS_ACTIVE`. Create new boundaries—or import existing
physical boundaries—through the [Admin boundary API](./admin#boundary-lifecycle)
before using EventStore methods. Requests to unknown or not-yet-installed
boundaries are rejected.
:::

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
import (
	"context"
	"log"

	orisun "github.com/oexza/orisun-client-go"
	eventstore "github.com/oexza/orisun-client-go/eventstore"
)

client, err := orisun.New(
	"localhost:5005",
	orisun.WithCredentials("admin", "changeit"),
	orisun.WithInsecure(), // plaintext transport; use WithTransportCredentials for TLS
)
if err != nil {
	log.Fatal(err)
}
defer client.Close()

ctx := context.Background()
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
import { EventStoreClient } from '@orisun/eventstore-client';

const client = new EventStoreClient({
  host: 'localhost',
  port: 5005,
  username: 'admin',
  password: 'changeit',
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
import com.orisunlabs.orisun.client.OrisunClient;
import com.orisunlabs.orisun.client.EventSubscription;
import com.orisun.eventstore.Eventstore;

try (OrisunClient client = OrisunClient.newBuilder()
    .withServer("localhost", 5005)
    .withBasicAuth("admin", "changeit")
    .build()) {
  // use client; Eventstore.* holds the generated message types
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
```

Send the header on every call:

```bash
grpcurl -H "$AUTH" localhost:5005 orisun.EventStore/Ping
```

  </TabItem>
</Tabs>

## Data model

Events have four caller-supplied fields:

| Field | Description |
| --- | --- |
| `event_id` | Stable event identifier. Use UUIDs for portability; the docs use UUIDv7 examples. PostgreSQL requires UUID format, while SQLite accepts any string. Orisun does not deduplicate writes by `event_id`; use it for application-level retry recognition and consumer deduplication. |
| `event_type` | Event type name, for example `OrderPlaced`. |
| `data` | JSON object encoded as a string. Criteria queries match this JSON object. |
| `metadata` | JSON object encoded as a string. Use for request source, tracing, or non-domain metadata. |

Orisun also stores a durable `position` and `date_created` on committed events.

:::note
Storage backends expose the event envelope through reserved fields in the
queryable document:

| Document field | Envelope value |
| --- | --- |
| `__eventId` | Event ID |
| `__eventType` | Event type |
| `__commitPosition` | Commit position, as an integer |
| `__preparePosition` | Prepare position, as an integer |
| `__writeId` | ID of the write that committed the event |
| `__dateCreated` | Creation timestamp, in UTC RFC 3339 format |
| `__metadata` | The metadata JSON value |

PostgreSQL and SQLite persist these values inside `data`. Their ordering and
write-context columns are generated projections of that document, not separate
writable values. PostgreSQL retains `pg_xact_id` as internal visibility bookkeeping.
FoundationDB stores metadata and timestamps inside the document and derives
positions from its native commit-ordered key. It derives write IDs from that key
and the stored batch-end offset; no second transaction fills in positions.

On retrieval, the backend extracts the usual envelope and removes **all
top-level `__*` fields** from returned application `data`. Nested fields and
metadata values remain untouched. API and SDK event shapes stay unchanged.
Content criteria and live subscription filters use reserved names, for example
`{"key":"__eventId","value":"your-event-id"}`. Metadata is a JSON value under
`__metadata`; this does not introduce dotted-path querying into nested objects.

FoundationDB uses its native event-key range for criteria containing
`__commitPosition` or `__writeId`, with any remaining tags applied within that
range. Ordered `__commitPosition` predicates bound that native range;
non-equality `__writeId` predicates can scan the full native event range because
write IDs compare as strings. A `__preparePosition` criterion needs one of those
anchors. These three
fields cannot be secondary-index fields or conditions. Other FoundationDB
criteria continue to require a ready covering index.

Top-level keys in application event `data` beginning with `__` are reserved for
Orisun, including names not currently in use. Writes containing such keys are
rejected before storage (with `INVALID_ARGUMENT` over gRPC). This restriction
applies only to the root of `data`: nested objects and the separate `metadata`
field may contain keys beginning with `__`. The write-request `event_type` field (or SDK `eventType` property) supplies
`__eventType`; application payloads must not set it themselves. An ordinary
application key named `eventType` is preserved and is no longer the discriminator.
:::

### Upgrading stored event fields

Follow the [event-envelope upgrade guide](../operations/upgrading-event-envelope)
for preparation, rollout, verification, failure recovery, and rollback steps.

Content queries and index definitions now use `__eventType` instead of
`eventType`. The API `event_type` field and SDK `eventType` property keep their
existing names. There is no query-time alias for the old JSON key.

Stop all Orisun servers sharing the storage before upgrading. At startup,
Orisun migrates stored event discriminators, retained CCC observations, and
index definitions created through the index API. PostgreSQL and SQLite rebuild
affected managed indexes within their migration transaction. FoundationDB
migrates in resumable batches before making the boundary available; its index
entries retain the same values and positions. Existing positions, write IDs,
and publisher checkpoints are preserved. The upgrade also moves stored event IDs
into `data.__eventId`, removing the separate PostgreSQL/SQLite `event_id` column
and FoundationDB record field. The remaining envelope migration replaces SQL
columns with generated projections and rebuilds their indexes in the same
transaction. FoundationDB migrates metadata, timestamps, and batch-end offsets
in resumable batches, maintaining affected indexes. Historical events whose write
context was never recorded continue to return an empty write ID. Large stores
may take time to migrate.

Update application criteria, subscription filters, and index declarations to
`__eventType` before resuming traffic. Re-read command contexts after the upgrade;
in-flight observations using the old key must not be reused. Indexes created
directly with SQL are not managed by Orisun and must be reviewed separately.
Older server binaries cannot be used with the migrated storage; take a backup
before upgrading if you need to be able to restore the old format.

If a legacy event already contains both `eventType` and `__eventType`, migration
stops with a conflict instead of overwriting either value. Existing top-level
`__eventId` values also block the event-ID migration, including JSON null. The
remaining-envelope migration likewise rejects existing `__commitPosition`,
`__preparePosition`, `__writeId`, `__dateCreated`, or `__metadata` fields;
FoundationDB also reserves `__writeLastOffset` for its batch-end offset. Existing
FoundationDB secondary indexes on commit-derived fields must be removed before
that migration. Resolve any collision
while the servers are stopped, then restart. Nested application fields and
metadata are not renamed. Historical `eventType` was the store discriminator;
after migration that unprefixed name is available for application data.

## SaveEventsV2

`SaveEventsV2` atomically appends one or more events. Its `consistency` list contains the complete query and latest matching position for every context the command read. Orisun validates all observations in the write transaction before inserting any event.

Atomicity is scoped to the request's one boundary. Orisun does not provide a
cross-boundary transaction; events that must commit together belong in the
same boundary and the same request.

| Field | Required | Meaning |
| --- | --- | --- |
| `boundary` | Yes | Active boundary receiving the complete event batch. |
| `events` | Yes | One or more events committed atomically. |
| `consistency` | No | Query-level observations that must all still be current. Omit it for an unconditional append. |

Each observation has one non-empty `query` and one `position`. Tags inside a
criterion are **AND** predicates; criteria inside a query are **OR**
alternatives. The position belongs to that whole OR query. It is never a
position per criterion.

The command lifecycle is:

1. Read each complete context needed for the decision.
2. Preserve each exact query with that query's latest matching position.
3. Apply domain validation in the command handler.
4. Send the new events and all preserved observations in one `SaveEventsV2`
   request.
5. On `ALREADY_EXISTS`, re-read every context and make the decision again.

The reads may be separate. The save is the synchronization point: every
observation is rechecked atomically with the append.

### Preserve a `GetLatestByCriteria` read

```go
criteria := []*eventstore.Criterion{
	{Tags: []*eventstore.Tag{{Key: "scopes.orderId", Value: "order-17"}}},
	{Tags: []*eventstore.Tag{
		{Key: "__eventType", Value: "CustomerOrderingSuspended"},
		{Key: "customerId", Value: "customer-4"},
	}},
}
latest, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
	Boundary: "orders",
	Criteria: criteria,
})
if err != nil {
	return err
}

_, err = client.SaveEventsV2(ctx, &eventstore.SaveEventsV2Request{
	Boundary: "orders",
	Events: []*eventstore.EventToSave{{
		EventId:   "018f2d5e-0002-7000-8000-000000000002",
		EventType: "OrderConfirmed",
		Data:      `{"orderId":"order-17","customerId":"customer-4","scopes.orderId":"order-17"}`,
		Metadata:  `{}`,
	}},
	Consistency: []*eventstore.ConsistencyObservation{{
		Query:    &eventstore.Query{Criteria: criteria},
		Position: latest.ContextPosition,
	}},
})
```

`GetLatestByCriteria` remains unchanged: combine the exact criteria sent to it with its existing `context_position` to construct the V2 observation. One position belongs to the complete OR query, not to each criterion.

### Preserve multiple independent reads

For multiple independently read contexts, include multiple observations:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEventsV2 <<EOF
{
  "boundary": "orders",
  "events": [{
    "event_id": "018f2d5e-0002-7000-8000-000000000002",
    "event_type": "OrderConfirmed",
    "data": "{\"orderId\":\"order-17\",\"customerId\":\"customer-4\",\"scopes.orderId\":\"order-17\"}",
    "metadata": "{}"
  }],
  "consistency": [
    {
      "query": {
        "criteria": [
          {"tags": [{"key": "scopes.orderId", "value": "order-17"}]},
          {"tags": [
            {"key": "__eventType", "value": "OrderCancelled"},
            {"key": "orderId", "value": "order-17"}
          ]}
        ]
      },
      "position": {"commit_position": 12, "prepare_position": 8}
    },
    {
      "query": {
        "criteria": [
          {"tags": [
            {"key": "__eventType", "value": "CustomerOrderingSuspended"},
            {"key": "customerId", "value": "customer-4"}
          ]},
          {"tags": [
            {"key": "__eventType", "value": "CustomerOrderingRestored"},
            {"key": "customerId", "value": "customer-4"}
          ]}
        ]
      },
      "position": {"commit_position": 9, "prepare_position": 0}
    }
  ]
}
EOF
```

Every observation is required to contain a non-empty query and a position. Use
`{-1, -1}` for a query observed to have no matches. Duplicate equivalent
observations are normalized, contradictory positions for the same query are
rejected, and requests with excessive observation fan-out fail closed.

Batches are atomic. Events in one batch share the same commit position and receive increasing prepare positions. If any observation is stale, Orisun returns `ALREADY_EXISTS` and appends none of the events.

`WriteResult.log_position` is the position of the last event in the committed
batch. It is a write receipt, not a general-purpose CCC token. Reuse it as an
observation position only when you can prove that the same complete query was
observed and the last event in the batch is its latest match. Normally, derive
positions from `GetLatestByCriteria` or a complete `GetEvents` read.

### Tag comparison operators

A tag has `key`, `value`, and an optional `operator`. Supported operators are
`eq`, `ne`, `gt`, `gte`, `lt`, and `lte`. Omitting `operator` (or sending an empty
string) means `eq`, preserving existing queries. Unknown operators return
`INVALID_ARGUMENT`.

| Operator | Match |
| --- | --- |
| `eq` | Equal to the supplied value |
| `ne` | Different from the supplied value |
| `gt` / `gte` | Greater than / greater than or equal to |
| `lt` / `lte` | Less than / less than or equal to |

`value` remains a string. Equality uses the existing text representation of the
stored value: for example, numeric `10` and string `"10"` both match
`{"key":"amount","value":"10"}`. `ne` is the inverse for a present, non-null
value. Missing fields and JSON null never match any operator, including `ne`.

Ordered comparisons use the **stored JSON type**:

- Numbers compare numerically, without rounding through floating point. The
  target must be a JSON number such as `"10"`, `"-2.5"`, or `"1e3"`; an invalid
  numeric target does not match a numeric field.
- Strings compare lexicographically in UTF-8 byte order, independent of database
  locale. Thus numeric `10 > 2`, while string `"10" < "2"`.
- Booleans, arrays, objects, and null do not support ordered comparisons.

Tags within a criterion are ANDed, including multiple predicates on the same
key. Criteria are ORed. For example, this query selects amounts in `[10, 20)`:

```json
{
  "criteria": [{
    "tags": [
      {"key": "amount", "operator": "gte", "value": "10"},
      {"key": "amount", "operator": "lt", "value": "20"}
    ]
  }]
}
```

Use the same complete query for reads and the corresponding CCC observation.
Operators apply to `GetEvents`, `GetLatestByCriteria`, subscription catch-up and
live delivery, and consistency checks on writes. `GetWriteContext` retains the
predicates that were checked. Upgrade servers before clients start sending
non-equality operators; an older server does not understand this field.

Range predicates may inspect more candidates than equality predicates.
PostgreSQL and SQLite use exact decimal comparators for numeric ranges, so
existing numeric-cast indexes do not directly accelerate that comparison. FoundationDB still requires
a native position query or a ready covering index; it narrows a secondary-index
scan by leading equality fields, filters candidates, and applies the requested
position order and limit afterwards. Broad ranges can reach FoundationDB's
transaction limits. Prefer selective equality fields alongside ranges.

### Validation and limits

The server rejects the entire request with `INVALID_ARGUMENT` before touching
storage when any of these rules fail:

- `boundary` is empty, `events` is empty, or event JSON is invalid;
- an observation has no query or position;
- a query has no criteria, a criterion has no tags, or a tag has no key;
- a position is neither exactly `{-1, -1}` nor a pair of non-negative values;
- a tag uses an unsupported operator, or one criterion has conflicting `eq`
  predicates for the same key; or
- equivalent observations claim different positions.

Equivalent criteria, repeated identical tags, and duplicate observations with
the same position are normalized rather than evaluated repeatedly. One request
may contain at most 1,024 observations, 4,096 criteria across those
observations, and 16,384 tags across those criteria. These bounds prevent an
individual write from creating unbounded query fan-out.

PostgreSQL and SQLite can evaluate an unindexed content query correctly by
scanning. FoundationDB requires each criterion to select a native position range
through `__commitPosition` or `__writeId`, or have a ready covering secondary
index; otherwise it returns `FAILED_PRECONDITION`. See [Indexing](../concepts/indexing).

### Unconditional append

Omit `consistency` when the command did not read any event context. This is the
correct shape for ingestion, replay into a new boundary, and other deliberately
unconditional writes. Do not send an empty query or a position by itself.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
result, err := client.SaveEventsV2(ctx, &eventstore.SaveEventsV2Request{
	Boundary: "orders",
	Events: []*eventstore.EventToSave{
		{
			EventId:   "018f2d5e-0001-7000-8000-000000000001",
			EventType: "OrderPlaced",
			Data:      `{"customer_id":"c-1","amount":45}`,
			Metadata:  `{"source":"checkout"}`,
		},
	},
})

// result.LogPosition.CommitPosition / PreparePosition
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const result = await client.saveEventsV2({
  boundary: 'orders',
  events: [
    {
      eventId: '018f2d5e-0001-7000-8000-000000000001',
      eventType: 'OrderPlaced',
      data: { customer_id: 'c-1', amount: 45 },
      metadata: { source: 'checkout' },
    },
  ],
});

// result.logPosition.commitPosition / preparePosition
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.WriteResult result = client.saveEventsV2(
    Eventstore.SaveEventsV2Request.newBuilder()
        .setBoundary("orders")
        .addEvents(Eventstore.EventToSave.newBuilder()
            .setEventId("018f2d5e-0001-7000-8000-000000000001")
            .setEventType("OrderPlaced")
            .setData("{\"customer_id\":\"c-1\",\"amount\":45}")
            .setMetadata("{\"source\":\"checkout\"}")
            .build())
        .build());

// result.getLogPosition().getCommitPosition()
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEventsV2 <<EOF
{
  "boundary": "orders",
  "events": [
    {
      "event_id": "018f2d5e-0001-7000-8000-000000000001",
      "event_type": "OrderPlaced",
      "data": "{\"customer_id\":\"c-1\",\"amount\":45}",
      "metadata": "{\"source\":\"checkout\"}"
    }
  ]
}
EOF
```

  </TabItem>
</Tabs>

The response contains the position of the last committed event in the batch:

```json
{
  "log_position": {
    "commit_position": 1,
    "prepare_position": 0
  }
}
```

Orisun stores the API `event_type` value in event `data` as the canonical `__eventType` JSON key and derives returned event types from that key. You do not need to duplicate it in your payload, and later queries or indexes can match `__eventType` with normal content criteria.

For event-scoped models, put queryable scope keys in `data` as normal JSON keys, for example `scopes.coursePublishedId`, and index them like any other field. See [Event Scopes](../patterns/event-scopes) for the modeling pattern.

Batches are atomic. Events in one batch share the same commit position and receive increasing prepare positions.

### Save with one observed query

Put the exact query the command read and that query's latest matching position
in one observation. Use the `context_position` returned by
`GetLatestByCriteria`, or the latest matching position from a complete
`GetEvents` read. A store-head position or the position returned by an unrelated
save is not a substitute.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.SaveEventsV2(ctx, &eventstore.SaveEventsV2Request{
	Boundary: "orders",
	Consistency: []*eventstore.ConsistencyObservation{{
		Position: &eventstore.Position{CommitPosition: 12, PreparePosition: 8},
		Query: &eventstore.Query{
			Criteria: []*eventstore.Criterion{{
				Tags: []*eventstore.Tag{{Key: "customer_id", Value: "c-1"}},
			}},
		},
	}},
	Events: []*eventstore.EventToSave{{
		EventId:   "018f2d5e-0002-7000-8000-000000000002",
		EventType: "OrderConfirmed",
		Data:      `{"customer_id":"c-1","amount":45}`,
		Metadata:  `{}`,
	}},
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.saveEventsV2({
  boundary: 'orders',
  consistency: [{
    position: { commitPosition: 12, preparePosition: 8 },
    query: {
      criteria: [
        { tags: [{ key: 'customer_id', value: 'c-1' }] },
      ],
    },
  }],
  events: [
    {
      eventId: '018f2d5e-0002-7000-8000-000000000002',
      eventType: 'OrderConfirmed',
      data: { customer_id: 'c-1', amount: 45 },
    },
  ],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.saveEventsV2(Eventstore.SaveEventsV2Request.newBuilder()
    .setBoundary("orders")
    .addConsistency(Eventstore.ConsistencyObservation.newBuilder()
        .setPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(12).setPreparePosition(8))
        .setQuery(Eventstore.Query.newBuilder()
            .addCriteria(Eventstore.Criterion.newBuilder()
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("customer_id").setValue("c-1").build())
                .build())
            .build()))
    .addEvents(Eventstore.EventToSave.newBuilder()
        .setEventId("018f2d5e-0002-7000-8000-000000000002")
        .setEventType("OrderConfirmed")
        .setData("{\"customer_id\":\"c-1\",\"amount\":45}")
        .build())
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEventsV2 <<EOF
{
  "boundary": "orders",
  "consistency": [{
    "position": {
      "commit_position": 12,
      "prepare_position": 8
    },
    "query": {
      "criteria": [
        {
          "tags": [
            {"key": "customer_id", "value": "c-1"}
          ]
        }
      ]
    }
  }],
  "events": [
    {
      "event_id": "018f2d5e-0002-7000-8000-000000000002",
      "event_type": "OrderConfirmed",
      "data": "{\"customer_id\":\"c-1\",\"amount\":45}",
      "metadata": "{}"
    }
  ]
}
EOF
```

  </TabItem>
</Tabs>

If the latest event matching the query is no longer at the observed position,
Orisun returns `ALREADY_EXISTS`. Treat that as a CCC conflict: re-read every
context used by the command, decide again, and retry only if the command is
still valid.

## GetWriteContext

Every accepted save records the complete consistency observations checked for
that atomic batch. Each observation retains its full OR query and the position
observed before the write. Queries use the server's normalized representation;
redundant equivalent observations may be deduplicated.

`WriteResult.write_id` identifies the accepted save. Every event in that save
carries the same `Event.write_id`, including events returned by latest-by-criteria
reads and catch-up or live subscriptions. Treat the ID as an opaque string scoped
to its boundary. Separate saves retain separate IDs even when group commit puts
them in one database transaction.

Call `GetWriteContext` with:

```json
{
  "boundary": "orders",
  "write_id": "<write_id from the save result or event>"
}
```

Historical events without recorded write-context evidence have an empty `write_id`.
The storage upgrade preserves that absence; it does not invent observations.

The response contains `write_id` and `consistency`, an array of the same
`ConsistencyObservation` shape accepted by `SaveEventsV2`. An existing record with
an empty array means the save was unconditional.

Missing records return `NOT_FOUND`;
malformed IDs return `INVALID_ARGUMENT`. The boundary must be active.

The context is store-owned and commits atomically with its events. Rejected or
rolled-back saves leave no context record. PostgreSQL and SQLite store one record
per save alongside the event table. FoundationDB stores the context under the
save's final versionstamp, splitting large contexts into values within the same
transaction. Context storage counts toward FoundationDB's transaction-size
budget. No application metadata fields are reserved or rewritten for this feature.

## GetEvents

Read from the beginning:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
resp, err := client.GetEvents(ctx, &eventstore.GetEventsRequest{
	Boundary:     "orders",
	FromPosition: &eventstore.Position{CommitPosition: 0, PreparePosition: 0},
	Count:        100,
	Direction:    eventstore.Direction_ASC,
})

for _, event := range resp.Events {
	_ = event // event.Position is durable ordering within the boundary
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const events = await client.getEvents({
  boundary: 'orders',
  fromPosition: { commitPosition: 0, preparePosition: 0 },
  count: 100,
  direction: 'ASC',
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetEventsResponse resp = client.getEvents(
    Eventstore.GetEventsRequest.newBuilder()
        .setBoundary("orders")
        .setFromPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(0).setPreparePosition(0).build())
        .setCount(100)
        .setDirection(Eventstore.Direction.ASC)
        .build());

// resp.getEventsList()
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetEvents <<EOF
{
  "boundary": "orders",
  "from_position": {
    "commit_position": 0,
    "prepare_position": 0
  },
  "count": 100,
  "direction": "ASC"
}
EOF
```

  </TabItem>
</Tabs>

Read by criteria:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
resp, err := client.GetEvents(ctx, &eventstore.GetEventsRequest{
	Boundary: "orders",
	Query: &eventstore.Query{
		Criteria: []*eventstore.Criterion{{
			Tags: []*eventstore.Tag{{Key: "customer_id", Value: "c-1"}},
		}},
	},
	Count:     100,
	Direction: eventstore.Direction_ASC,
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const events = await client.getEvents({
  boundary: 'orders',
  query: {
    criteria: [
      { tags: [{ key: 'customer_id', value: 'c-1' }] },
    ],
  },
  count: 100,
  direction: 'ASC',
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetEventsResponse resp = client.getEvents(
    Eventstore.GetEventsRequest.newBuilder()
        .setBoundary("orders")
        .setQuery(Eventstore.Query.newBuilder()
            .addCriteria(Eventstore.Criterion.newBuilder()
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("customer_id").setValue("c-1").build())
                .build())
            .build())
        .setCount(100)
        .setDirection(Eventstore.Direction.ASC)
        .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetEvents <<EOF
{
  "boundary": "orders",
  "query": {
    "criteria": [
      {
        "tags": [
          {"key": "customer_id", "value": "c-1"}
        ]
      }
    ]
  },
  "count": 100,
  "direction": "ASC"
}
EOF
```

  </TabItem>
</Tabs>

Page from a position:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
resp, err := client.GetEvents(ctx, &eventstore.GetEventsRequest{
	Boundary:     "orders",
	FromPosition: &eventstore.Position{CommitPosition: 1000, PreparePosition: 42},
	Count:        100,
	Direction:    eventstore.Direction_ASC,
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const events = await client.getEvents({
  boundary: 'orders',
  fromPosition: { commitPosition: 1000, preparePosition: 42 },
  count: 100,
  direction: 'ASC',
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetEventsResponse resp = client.getEvents(
    Eventstore.GetEventsRequest.newBuilder()
        .setBoundary("orders")
        .setFromPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(1000).setPreparePosition(42).build())
        .setCount(100)
        .setDirection(Eventstore.Direction.ASC)
        .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetEvents <<EOF
{
  "boundary": "orders",
  "from_position": {
    "commit_position": 1000,
    "prepare_position": 42
  },
  "count": 100,
  "direction": "ASC"
}
EOF
```

  </TabItem>
</Tabs>

`GetEvents` returns matching events with their committed position and creation time:

```json
{
  "events": [
    {
      "event_id": "018f2d5e-0001-7000-8000-000000000001",
      "event_type": "OrderPlaced",
      "data": "{\"customer_id\":\"c-1\",\"amount\":45}",
      "metadata": "{\"source\":\"checkout\"}",
      "position": {"commit_position": 1, "prepare_position": 0},
      "date_created": "2026-05-30T12:00:00Z"
    }
  ]
}
```

`Event` adds `position` and `date_created` to the fields supplied at write time. `CatchUpSubscribeToEvents` delivers the same event shape.

### Paging through a boundary

`GetEvents` returns one bounded page (`count`, server-capped at 10000). To walk the whole log or a criteria set, page forward:

1. First call uses `from_position` `{0, 0}` to start at the beginning.
2. Process the page, then take the `position` of the last event.
3. Pass it as `from_position` on the next call.
4. Stop when a page returns fewer events than `count`.

Keep the consumer idempotent and deduplicate by `event_id` rather than assuming exactly-once paging. The position model behind `from_position` and `direction` is described in [Positions and Ordering](../concepts/positions).

### Use `GetEvents` as a command context

`GetEvents` does not return a separate context position. When a command truly
needs the matching history, finish reading the complete queried context and
retain the greatest event position returned. Pair that position with the exact
same query in one `SaveEventsV2` observation. If the query returned no events,
use `{-1, -1}`.

Do not build an observation from a truncated page, a page that stopped before
the newest match, or a boundary-wide read paired with a narrower query. For
carried-state models that need only the newest match per criterion, prefer
`GetLatestByCriteria`; it returns `context_position` directly from the same
snapshot as its results.

## GetLatestByCriteria

`GetLatestByCriteria` returns the latest event matching each criterion, assembled by the server from **one consistent read snapshot**, plus a `context_position`. It is the command-side read for the carried-state pattern: store the resulting state on each event, then a command needs only the latest event per criterion rather than a history replay.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
resp, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
	Boundary: "ledger",
	Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{
			{Key: "__eventType", Value: "AccountOpened"},
			{Key: "accountOpenedId", Value: "018f2d5e-2001-7000-8000-000000000001"},
		}},
		{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: "018f2d5e-2001-7000-8000-000000000001"}}},
		{Tags: []*eventstore.Tag{
			{Key: "__eventType", Value: "AccountOpened"},
			{Key: "accountOpenedId", Value: "018f2d5e-2002-7000-8000-000000000002"},
		}},
		{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: "018f2d5e-2002-7000-8000-000000000002"}}},
	},
})

// One result per criterion, in request order.
// Use the root event when no scoped movement exists yet.
// For SaveEventsV2, pair these exact request criteria with resp.ContextPosition.
for _, r := range resp.Results {
	if r.Event != nil {
		// r.Event.Data carries the latest snapshot for this criterion
	}
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const latest = await client.getLatestByCriteria({
  boundary: 'ledger',
  criteria: [
    { tags: [
      { key: '__eventType', value: 'AccountOpened' },
      { key: 'accountOpenedId', value: '018f2d5e-2001-7000-8000-000000000001' },
    ] },
    { tags: [{ key: 'scopes.accountOpenedId', value: '018f2d5e-2001-7000-8000-000000000001' }] },
    { tags: [
      { key: '__eventType', value: 'AccountOpened' },
      { key: 'accountOpenedId', value: '018f2d5e-2002-7000-8000-000000000002' },
    ] },
    { tags: [{ key: 'scopes.accountOpenedId', value: '018f2d5e-2002-7000-8000-000000000002' }] },
  ],
});

// latest.results[i].event: latest event per criterion, in request order
// For saveEventsV2, pair these exact request criteria with latest.contextPosition.
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetLatestByCriteriaResponse latest = client.getLatestByCriteria(
    Eventstore.GetLatestByCriteriaRequest.newBuilder()
        .setBoundary("ledger")
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("__eventType").setValue("AccountOpened").build())
            .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue("018f2d5e-2001-7000-8000-000000000001").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("scopes.accountOpenedId").setValue("018f2d5e-2001-7000-8000-000000000001").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("__eventType").setValue("AccountOpened").build())
            .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue("018f2d5e-2002-7000-8000-000000000002").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("scopes.accountOpenedId").setValue("018f2d5e-2002-7000-8000-000000000002").build())
            .build())
        .build());

// For SaveEventsV2, pair the exact request criteria with latest.getContextPosition().
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetLatestByCriteria <<EOF
{
  "boundary": "ledger",
  "criteria": [
    {"tags": [
      {"key": "__eventType", "value": "AccountOpened"},
      {"key": "accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}
    ]},
    {"tags": [{"key": "scopes.accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}]},
    {"tags": [
      {"key": "__eventType", "value": "AccountOpened"},
      {"key": "accountOpenedId", "value": "018f2d5e-2002-7000-8000-000000000002"}
    ]},
    {"tags": [{"key": "scopes.accountOpenedId", "value": "018f2d5e-2002-7000-8000-000000000002"}]}
  ]
}
EOF
```

  </TabItem>
</Tabs>

The response carries one `result` per request criterion in order (`event` unset when nothing matches) and `context_position`, which is the max position observed in the same snapshot, or `{-1, -1}` when nothing matched.

For `SaveEventsV2`, construct one observation from the exact combined criteria sent to this RPC and the returned `context_position`. Multiple complete reads may each contribute their own query-level observation because V2 validates all of them atomically. See [Command Context Consistency](../concepts/command-context-consistency).

## CatchUpSubscribeToEvents

Subscriptions read matching events from the backend throughout their lifetime.
NATS boundary hints trigger reads; an subscription-owned idle watchdog hints timer recovers
missed hints. The backend evaluates the complete query for both historical and
new events. See [Delivery Guarantees](../concepts/delivery-guarantees).

When `after_position` is omitted, the subscription delivers the latest stored
event matching the query once, then continues with newer matching events in
ascending position order. If no stored event matches, it waits for future
matches. An explicit `after_position` is exclusive: the event at that position
is not replayed. The delivery cursor advances only after successful delivery.

Only one active subscription may use the same boundary and subscriber-name
pair. Orisun holds a renewable JetStream lease for the complete catch-up and
live lifetime. Closing the stream releases it immediately; if the subscriber
or server disappears before cleanup completes, another subscriber can reclaim
the lease after its 15-second expiry. Reuse a subscriber name for failover of
the same logical consumer, and use distinct names for consumers that should run
concurrently.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
handler := orisun.NewSimpleEventHandler().
	WithOnEvent(func(event *eventstore.Event) error {
		// persist side effects, then checkpoint event.Position
		return nil
	}).
	WithOnError(func(err error) {
		log.Printf("subscription stopped: %v", err)
	})

sub, err := client.SubscribeToEvents(ctx, &eventstore.CatchUpSubscribeToEventStoreRequest{
	Boundary:       "orders",
	SubscriberName: "order-projector",
	AfterPosition:  &eventstore.Position{CommitPosition: 0, PreparePosition: 0},
}, handler)
if err != nil {
	return err
}
defer sub.Close()
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const subscription = client.subscribeToEvents(
  {
    subscriberName: 'order-projector',
    boundary: 'orders',
    afterPosition: { commitPosition: 0, preparePosition: 0 },
  },
  async (event) => {
    // persist side effects, then checkpoint event.position
    console.log('event:', event.eventType, event.data);
  },
  (error) => {
    console.error('subscription error:', error);
  },
);

// subscription.cancel() to stop
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
EventSubscription sub = client.subscribeToEvents(
    Eventstore.CatchUpSubscribeToEventStoreRequest.newBuilder()
        .setBoundary("orders")
        .setSubscriberName("order-projector")
        .setAfterPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(0).setPreparePosition(0).build())
        .build(),
    new EventSubscription.EventHandler() {
        public void onEvent(Eventstore.Event event) { /* project + checkpoint */ }
        public void onError(Throwable error) { error.printStackTrace(); }
        public void onCompleted() {}
    });

// sub.close() to stop
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CatchUpSubscribeToEvents <<EOF
{
  "subscriber_name": "order-projector",
  "boundary": "orders",
  "after_position": {
    "commit_position": 0,
    "prepare_position": 0
  }
}
EOF
```

  </TabItem>
</Tabs>

Filtered subscription:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
sub, err := client.SubscribeToEvents(ctx, &eventstore.CatchUpSubscribeToEventStoreRequest{
	Boundary:       "orders",
	SubscriberName: "placed-orders",
	AfterPosition:  &eventstore.Position{CommitPosition: 0, PreparePosition: 0},
	Query: &eventstore.Query{
		Criteria: []*eventstore.Criterion{{
			Tags: []*eventstore.Tag{{Key: "__eventType", Value: "OrderPlaced"}},
		}},
	},
}, handler)
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const subscription = client.subscribeToEvents(
  {
    subscriberName: 'placed-orders',
    boundary: 'orders',
    afterPosition: { commitPosition: 0, preparePosition: 0 },
    query: {
      criteria: [
        { tags: [{ key: '__eventType', value: 'OrderPlaced' }] },
      ],
    },
  },
  async (event) => { /* ... */ },
);
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.subscribeToEvents(Eventstore.CatchUpSubscribeToEventStoreRequest.newBuilder()
        .setBoundary("orders")
        .setSubscriberName("placed-orders")
        .setAfterPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(0).setPreparePosition(0).build())
        .setQuery(Eventstore.Query.newBuilder()
            .addCriteria(Eventstore.Criterion.newBuilder()
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("__eventType").setValue("OrderPlaced").build())
                .build())
            .build())
        .build(),
    /* handler */);
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CatchUpSubscribeToEvents <<EOF
{
  "subscriber_name": "placed-orders",
  "boundary": "orders",
  "after_position": {
    "commit_position": 0,
    "prepare_position": 0
  },
  "query": {
    "criteria": [
      {
        "tags": [
          {"key": "__eventType", "value": "OrderPlaced"}
        ]
      }
    ]
  }
}
EOF
```

  </TabItem>
</Tabs>

## Ping

`Ping` is an authenticated liveness check that takes no arguments:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
if err := client.Ping(ctx); err != nil {
	return err
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.ping();
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.ping();
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d '{}' localhost:5005 orisun.EventStore/Ping
```

  </TabItem>
</Tabs>

## GetServerInfo

`GetServerInfo` returns information about the node that handles the call. It is
authenticated but does not require a particular role.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
info, err := client.GetServerInfo(ctx)
if err != nil {
	return err
}
log.Printf("node=%s version=%s backend=%s",
	info.NodeId, info.Version, info.Backend)
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const info = await client.getServerInfo();
console.log(info.nodeId, info.version, info.backend, info.capabilities);
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetServerInfoResponse info = client.getServerInfo();
System.out.printf("node=%s version=%s backend=%s%n",
    info.getNodeId(), info.getVersion(), info.getBackend());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d '{}' \
  localhost:5005 orisun.EventStore/GetServerInfo
```

  </TabItem>
</Tabs>

The response contains:

| Field | Meaning |
| --- | --- |
| `version` | Orisun release version embedded at build time. Local development builds report `dev`. |
| `git_commit` | Source commit embedded at build time, or `unknown`. |
| `build_time` | Build timestamp embedded by the release build, or `unknown`. |
| `backend` | `STORAGE_BACKEND_POSTGRES`, `STORAGE_BACKEND_SQLITE`, or `STORAGE_BACKEND_FOUNDATIONDB`. |
| `node_id` | UUID for this running server process. It changes when the process restarts. |
| `capabilities` | Typed features supported by the connected server. |

Capabilities currently report Command Context Consistency, catch-up
subscriptions, index management, the boundary catalog, and standard gRPC
health. Clients should check for the capability they need instead of inferring
support from the version string.

## CreateIndex

On PostgreSQL and SQLite, repeating the same definition is idempotent. An
existing logical name or physical index with a different definition returns
`ALREADY_EXISTS`. To change a definition, call `DropIndex` first; a conflicting
`CreateIndex` never overwrites the existing definition.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "orders",
	Name:     "customer_id",
	Fields: []*eventstore.IndexField{{
		JsonKey:   "customer_id",
		ValueType: eventstore.ValueType_TEXT,
	}},
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.createIndex({
  boundary: 'orders',
  name: 'customer_id',
  fields: [
    { jsonKey: 'customer_id', valueType: 'TEXT' },
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
        .setValueType(Eventstore.ValueType.TEXT)
        .build())
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
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

`value_type` is `TEXT`, `NUMERIC`, `BOOLEAN`, or `TIMESTAMPTZ`. Add `conditions` for a partial index. Each condition `operator` must be one of `=`, `>`, `<`, `>=`, or `<=`. See [Indexing](../concepts/indexing) for composite and partial index examples.

## ListIndexes and GetIndex

Both calls return Orisun-managed index definitions, including fields, partial
conditions, the condition combinator, and `BUILDING` or `READY` state.
`GetIndex` returns `NOT_FOUND` when the logical name is not registered.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
list, err := client.ListIndexes(ctx, "orders")
one, err := client.GetIndex(ctx, "orders", "customer_id")
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const list = await client.listIndexes('orders');
const one = await client.getIndex('orders', 'customer_id');
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.ListIndexesResponse list = client.listIndexes("orders");
Eventstore.GetIndexResponse one = client.getIndex("orders", "customer_id");
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d '{"boundary":"orders"}' \
  localhost:5005 orisun.EventStore/ListIndexes

grpcurl -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/GetIndex
```

  </TabItem>
</Tabs>

`ADMIN` and `OPERATIONS` users can inspect indexes. Creating and dropping
indexes remains restricted to `ADMIN`.

## DropIndex

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.DropIndex(ctx, &eventstore.DropIndexRequest{
	Boundary: "orders",
	Name:     "customer_id",
})
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
grpcurl -H "$AUTH" \
  -d '{"boundary":"orders","name":"customer_id"}' \
  localhost:5005 orisun.EventStore/DropIndex
```

  </TabItem>
</Tabs>

## Handling consistency conflicts

When any command context changed between read and write, `SaveEventsV2` returns `ALREADY_EXISTS`. Treat it as a retryable business conflict: re-read every context needed by the decision, re-decide, and save again with fresh observations.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
var conflict *orisun.OptimisticConcurrencyException
if errors.As(err, &conflict) {
	log.Printf("consistency conflict: expected=%v actual=%v",
		conflict.ExpectedVersion(), conflict.ActualVersion())
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
try {
  await client.saveEventsV2({ /* ... */ });
} catch (error) {
  if (error.message.includes('AlreadyExists')) {
    // Concurrency conflict. Re-read the context and retry.
  } else {
    throw error;
  }
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
try {
    client.saveEventsV2(request);
} catch (OptimisticConcurrencyException conflict) {
    // Concurrency conflict. Re-read the context and retry.
    // conflict.getExpectedVersion() / conflict.getActualVersion()
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```text
ERROR:
  Code: AlreadyExists
```

  </TabItem>
</Tabs>

## Proto source

The EventStore protobuf source lives at [`proto/eventstore.proto`](https://github.com/OrisunLabs/Orisun/blob/main/proto/eventstore.proto).

## Common status codes

| Status | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | The request is malformed, uses invalid JSON, or references invalid index fields. |
| `UNAUTHENTICATED` | Missing or invalid credentials. |
| `PERMISSION_DENIED` | Authenticated user does not have a required role. |
| `FAILED_PRECONDITION` | The boundary is not active, or a FoundationDB criterion has neither a native position range nor a ready covering secondary index. |
| `ALREADY_EXISTS` | One or more observations changed during `SaveEventsV2`; re-query and retry if still valid. |
| `INTERNAL` | Storage, publishing, or unexpected server failure. |
