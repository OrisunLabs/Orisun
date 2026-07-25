---
title: EventStore API
description: Save, query, subscribe, ping, and manage indexes.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The EventStore service owns event operations:

- `SaveEvents`
- `GetEvents`
- `GetLatestByCriteria`
- `CatchUpSubscribeToEvents`
- `Ping`
- `CreateIndex`
- `DropIndex`

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
`event_type` is the API field for the event's type. On save, Orisun writes that value into the stored event data as the canonical `eventType` JSON key, and storage backends derive response `event_type` from `data.eventType`. Criteria and indexes should use the `eventType` JSON key.
:::

## SaveEvents

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
result, err := client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
	Boundary: "orders",
	Query: &eventstore.SaveQuery{
		ExpectedPosition: &eventstore.Position{CommitPosition: -1, PreparePosition: -1},
	},
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
const result = await client.saveEvents({
  boundary: 'orders',
  query: {
    expectedPosition: { commitPosition: -1, preparePosition: -1 },
  },
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
Eventstore.WriteResult result = client.saveEvents(
    Eventstore.SaveEventsRequest.newBuilder()
        .setBoundary("orders")
        .setQuery(Eventstore.SaveQuery.newBuilder()
            .setExpectedPosition(Eventstore.Position.newBuilder()
                .setCommitPosition(-1).setPreparePosition(-1).build())
            .build())
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
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEvents <<EOF
{
  "boundary": "orders",
  "query": {
    "expected_position": {
      "commit_position": -1,
      "prepare_position": -1
    }
  },
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

The response contains the committed log position:

```json
{
  "log_position": {
    "commit_position": 1,
    "prepare_position": 0
  }
}
```

Orisun stores the API `event_type` value in event `data` as the canonical `eventType` JSON key and derives returned event types from that key. You do not need to duplicate it in your payload, and later queries or indexes can match `eventType` with normal content criteria.

For event-scoped models, put queryable scope keys in `data` as normal JSON keys, for example `scopes.coursePublishedId`, and index them like any other field. See [Event Scopes](../patterns/event-scopes) for the modeling pattern.

Batches are atomic. Events in one batch share the same commit position and receive increasing prepare positions.

### Save with a consistency subset

Use `query.subsetQuery` to enforce Command Context Consistency for a specific event subset:

`subsetQuery` defines the dynamic set of events the command depends on, and `expected_position` is the position of the latest matching event observed when that set was read. The append succeeds only if the latest event matching `subsetQuery` is still at exactly `expected_position`. Use the `context_position` returned by `GetLatestByCriteria`, or the latest matching position from a complete `GetEvents` read; an arbitrary later position such as the store head is not a valid substitute.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err := client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
	Boundary: "orders",
	Query: &eventstore.SaveQuery{
		ExpectedPosition: &eventstore.Position{CommitPosition: 12, PreparePosition: 8},
		SubsetQuery: &eventstore.Query{
			Criteria: []*eventstore.Criterion{{
				Tags: []*eventstore.Tag{{Key: "customer_id", Value: "c-1"}},
			}},
		},
	},
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
await client.saveEvents({
  boundary: 'orders',
  query: {
    expectedPosition: { commitPosition: 12, preparePosition: 8 },
    subsetQuery: {
      criteria: [
        { tags: [{ key: 'customer_id', value: 'c-1' }] },
      ],
    },
  },
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
client.saveEvents(Eventstore.SaveEventsRequest.newBuilder()
    .setBoundary("orders")
    .setQuery(Eventstore.SaveQuery.newBuilder()
        .setExpectedPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(12).setPreparePosition(8).build())
        .setSubsetQuery(Eventstore.Query.newBuilder()
            .addCriteria(Eventstore.Criterion.newBuilder()
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("customer_id").setValue("c-1").build())
                .build())
            .build())
        .build())
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
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEvents <<EOF
{
  "boundary": "orders",
  "query": {
    "expected_position": {
      "commit_position": 12,
      "prepare_position": 8
    },
    "subsetQuery": {
      "criteria": [
        {
          "tags": [
            {"key": "customer_id", "value": "c-1"}
          ]
        }
      ]
    }
  },
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

If the latest event matching the subset is no longer at the expected position, Orisun returns `ALREADY_EXISTS`. Treat that as a CCC conflict: re-read the context, decide again, and retry only if the command is still valid.

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

## GetLatestByCriteria

`GetLatestByCriteria` returns the latest event matching each criterion, assembled by the server from **one consistent read snapshot**, plus a `context_position` to use as the `expected_position` of the next `SaveEvents` with the same combined criteria. It is the command-side read for the carried-state pattern: store the resulting state (for example an account balance) on each event, then a command needs only the latest event per entity, not a history replay.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
resp, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
	Boundary: "ledger",
	Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{
			{Key: "eventType", Value: "AccountOpened"},
			{Key: "accountOpenedId", Value: "018f2d5e-2001-7000-8000-000000000001"},
		}},
		{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: "018f2d5e-2001-7000-8000-000000000001"}}},
		{Tags: []*eventstore.Tag{
			{Key: "eventType", Value: "AccountOpened"},
			{Key: "accountOpenedId", Value: "018f2d5e-2002-7000-8000-000000000002"},
		}},
		{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: "018f2d5e-2002-7000-8000-000000000002"}}},
	},
})

// One result per criterion, in request order.
// Use the root event when no scoped movement exists yet.
// resp.ContextPosition is the next expected_position for a SaveEvents
// using the same combined criteria.
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
      { key: 'eventType', value: 'AccountOpened' },
      { key: 'accountOpenedId', value: '018f2d5e-2001-7000-8000-000000000001' },
    ] },
    { tags: [{ key: 'scopes.accountOpenedId', value: '018f2d5e-2001-7000-8000-000000000001' }] },
    { tags: [
      { key: 'eventType', value: 'AccountOpened' },
      { key: 'accountOpenedId', value: '018f2d5e-2002-7000-8000-000000000002' },
    ] },
    { tags: [{ key: 'scopes.accountOpenedId', value: '018f2d5e-2002-7000-8000-000000000002' }] },
  ],
});

// latest.results[i].event: latest event per criterion, in request order
// latest.contextPosition: pass to the next saveEvents expectedPosition
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetLatestByCriteriaResponse latest = client.getLatestByCriteria(
    Eventstore.GetLatestByCriteriaRequest.newBuilder()
        .setBoundary("ledger")
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("eventType").setValue("AccountOpened").build())
            .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue("018f2d5e-2001-7000-8000-000000000001").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("scopes.accountOpenedId").setValue("018f2d5e-2001-7000-8000-000000000001").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("eventType").setValue("AccountOpened").build())
            .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue("018f2d5e-2002-7000-8000-000000000002").build())
            .build())
        .addCriteria(Eventstore.Criterion.newBuilder()
            .addTags(Eventstore.Tag.newBuilder().setKey("scopes.accountOpenedId").setValue("018f2d5e-2002-7000-8000-000000000002").build())
            .build())
        .build());

Eventstore.Position expectedPosition = latest.getContextPosition();
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetLatestByCriteria <<EOF
{
  "boundary": "ledger",
  "criteria": [
    {"tags": [
      {"key": "eventType", "value": "AccountOpened"},
      {"key": "accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}
    ]},
    {"tags": [{"key": "scopes.accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}]},
    {"tags": [
      {"key": "eventType", "value": "AccountOpened"},
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

Why a dedicated RPC instead of separate `GetEvents` calls: two calls are two snapshots. An event can commit between them with a position *below* the maximum you observed, and a scalar `expected_position` can only prove "nothing newer than X exists." It cannot prove your reads saw everything up to X. `GetLatestByCriteria` closes that gap by sampling the whole context atomically. See [Command Context Consistency](../concepts/command-context-consistency).

## CatchUpSubscribeToEvents

Catch-up subscriptions replay stored events, then switch to live JetStream delivery.

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
  (event) => {
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
			Tags: []*eventstore.Tag{{Key: "eventType", Value: "OrderPlaced"}},
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
        { tags: [{ key: 'eventType', value: 'OrderPlaced' }] },
      ],
    },
  },
  (event) => { /* ... */ },
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
                    .setKey("eventType").setValue("OrderPlaced").build())
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
          {"key": "eventType", "value": "OrderPlaced"}
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

## CreateIndex

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

When a command's context changed between read and write, `SaveEvents` returns `ALREADY_EXISTS`. Treat it as a retryable business conflict: re-read the context, re-decide, and save again with the new position.

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
  await client.saveEvents({ /* ... */ });
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
    client.saveEvents(request);
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
| `ALREADY_EXISTS` | Optimistic consistency conflict during `SaveEvents`; re-query and retry if still valid. |
| `INTERNAL` | Storage, publishing, or unexpected server failure. |
