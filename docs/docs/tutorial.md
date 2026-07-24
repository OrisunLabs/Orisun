---
id: tutorial
title: "Tutorial: A Consistent Ledger"
description: Build an account ledger end to end with Command Context Consistency, indexes, and a live subscription.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This tutorial builds a small account ledger and walks through the core Orisun workflow: save events, scope a consistency check with a query, handle a concurrency conflict, index the query field, and subscribe a projector to live updates.

Start a server with [Getting Started](./getting-started) before continuing. The examples below use the default `admin:changeit` credentials and the `accounts` boundary.

## The scenario

Money moves between two accounts by transfer. A transfer posts two events in the same atomic write: a debit on the source account and a credit on the destination account. That is the double-entry invariant: the two legs of a transfer commit together or not at all, so the ledger's total balance never drifts. The additional invariant: an account must not be debited below zero. If two transfers are decided from the same source balance, both must not commit. Command Context Consistency gives the application that protection.

Following the [scoping events](./patterns/event-scopes) pattern, an account has no separate entity identity. It *is* its `AccountOpened` event. Later events reference that event's own id, not a hand-rolled `account_id` foreign key. See [Command Context Consistency](./concepts/command-context-consistency#example-context) for the same `accountOpenedId` / `scopes.*AccountOpenedId` convention used here.

The ledger uses an `accounts` boundary. Create it through the Admin API after
the server starts:

```bash
grpcurl -plaintext \
  -H 'Authorization: Basic YWRtaW46Y2hhbmdlaXQ=' \
  -d '{"name":"accounts","description":"ledger","placement":{"backend":"postgres","namespace":"public"}}' \
  localhost:5005 orisun.Admin/CreateBoundary
```

The command returns while the boundary is `PROVISIONING`. Use `GetBoundary` and
continue once its status is `BOUNDARY_LIFECYCLE_STATUS_ACTIVE`.

Only the admin schema is configured at startup for a fresh
PostgreSQL-compatible deployment, including YugabyteDB:

```bash
ORISUN_PG_ADMIN_SCHEMA=admin
```

## Connect

Pick your client once; every step below follows that choice. See [EventStore API](./api/eventstore#connect-and-authenticate) for full connection options.

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
	orisun.WithInsecure(),
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
  // steps below run inside this block
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
```

  </TabItem>
</Tabs>

## 1. Open two accounts

`AccountOpened` is a scope root: its own event id becomes the account's identity, carried in the payload as `accountOpenedId`. There is no `Account` row anywhere and no separately-assigned business key; the event *is* the account. The first event for a brand-new scope uses the before-first-event position `{-1, -1}`.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err = client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
	Boundary: "accounts",
	Query: &eventstore.SaveQuery{
		ExpectedPosition: &eventstore.Position{CommitPosition: -1, PreparePosition: -1},
		SubsetQuery: &eventstore.Query{
			Criteria: []*eventstore.Criterion{{
				Tags: []*eventstore.Tag{
					{Key: "eventType", Value: "AccountOpened"},
					{Key: "accountOpenedId", Value: "018f2d5e-2001-7000-8000-000000000001"},
				},
			}},
		},
	},
	Events: []*eventstore.EventToSave{{
		EventId:   "018f2d5e-2001-7000-8000-000000000001",
		EventType: "AccountOpened",
		Data:      `{"accountOpenedId":"018f2d5e-2001-7000-8000-000000000001","balance":100}`,
		Metadata:  `{}`,
	}},
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.saveEvents({
  boundary: 'accounts',
  query: {
    expectedPosition: { commitPosition: -1, preparePosition: -1 },
    subsetQuery: {
      criteria: [{
        tags: [
          { key: 'eventType', value: 'AccountOpened' },
          { key: 'accountOpenedId', value: '018f2d5e-2001-7000-8000-000000000001' },
        ],
      }],
    },
  },
  events: [
    {
      eventId: '018f2d5e-2001-7000-8000-000000000001',
      eventType: 'AccountOpened',
      data: { accountOpenedId: '018f2d5e-2001-7000-8000-000000000001', balance: 100 },
    },
  ],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.saveEvents(Eventstore.SaveEventsRequest.newBuilder()
    .setBoundary("accounts")
    .setQuery(Eventstore.SaveQuery.newBuilder()
        .setExpectedPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(-1).setPreparePosition(-1).build())
        .setSubsetQuery(Eventstore.Query.newBuilder()
            .addCriteria(Eventstore.Criterion.newBuilder()
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("eventType").setValue("AccountOpened").build())
                .addTags(Eventstore.Tag.newBuilder()
                    .setKey("accountOpenedId").setValue("018f2d5e-2001-7000-8000-000000000001").build())
                .build())
            .build())
        .build())
    .addEvents(Eventstore.EventToSave.newBuilder()
        .setEventId("018f2d5e-2001-7000-8000-000000000001")
        .setEventType("AccountOpened")
        .setData("{\"accountOpenedId\":\"018f2d5e-2001-7000-8000-000000000001\",\"balance\":100}")
        .build())
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEvents <<EOF
{
  "boundary": "accounts",
  "query": {
    "expected_position": {"commit_position": -1, "prepare_position": -1},
    "subsetQuery": {
      "criteria": [
        {"tags": [
          {"key": "eventType", "value": "AccountOpened"},
          {"key": "accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}
        ]}
      ]
    }
  },
  "events": [
    {
      "event_id": "018f2d5e-2001-7000-8000-000000000001",
      "event_type": "AccountOpened",
      "data": "{\"accountOpenedId\":\"018f2d5e-2001-7000-8000-000000000001\",\"balance\":100}",
      "metadata": "{}"
    }
  ]
}
EOF
```

  </TabItem>
</Tabs>

Repeat with a fresh event id to open the transfer's destination account, using that same id as its own `accountOpenedId`:

```json
{"accountOpenedId": "018f2d5e-2002-7000-8000-000000000002", "balance": 0}
```

using event id `018f2d5e-2002-7000-8000-000000000002`. The two opens are independent consistency contexts, so they can run concurrently.

The stored event data includes canonical `eventType` from the caller-supplied API `event_type`, and Orisun derives returned event types from that JSON key. Later queries and indexes can filter by event type without adding it to each `data` JSON payload.

## 2. Read both balances from one snapshot

A transfer's context spans both accounts: the source balance decides whether the debit is valid, and both accounts must not have moved since the read. Each account needs two criteria, its `AccountOpened` root and any later event scoped to it via `scopes.accountOpenedId`, read from a single server-side snapshot:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
criteria := []*eventstore.Criterion{
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
}

resp, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
	Boundary: "accounts",
	Criteria: criteria,
})

// resp.Results[0] is acct-1's AccountOpened event, resp.Results[1] is its latest movement (if any).
// resp.Results[2] is acct-2's AccountOpened event, resp.Results[3] is its latest movement (if any).
// Each account's current balance is the movement's balanceAfter when present, else the root's balance.
// resp.ContextPosition is the consistency anchor for the transfer.
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const latest = await client.getLatestByCriteria({
  boundary: 'accounts',
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

const [fromOpened, fromMovement, toOpened, toMovement] = latest.results;
const fromBalance = fromMovement.event?.data.balanceAfter ?? fromOpened.event.data.balance;
const toBalance = toMovement.event?.data.balanceAfter ?? toOpened.event.data.balance;
const expectedPosition = latest.contextPosition; // consistency anchor for the transfer
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Eventstore.GetLatestByCriteriaResponse latest = client.getLatestByCriteria(
    Eventstore.GetLatestByCriteriaRequest.newBuilder()
        .setBoundary("accounts")
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

// results 0/1 are acct-1's root and latest movement, results 2/3 are acct-2's.
// latest.getContextPosition() is the consistency anchor for the transfer.
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/GetLatestByCriteria <<EOF
{
  "boundary": "accounts",
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

The `scopes.accountOpenedId` criterion deliberately omits an `eventType` tag: it matches any later event scoped to that account, regardless of whether it is a debit or a credit, so the response always carries that account's freshest movement. The application derives each balance, decides whether the transfer is valid, and remembers `context_position` as the consistency anchor for the write, reusing these same four criteria. See [Command Context Consistency](./concepts/command-context-consistency#reading-a-command-context) for why independent per-account reads cannot substitute for this.

## 3. Transfer with a double-entry write

The transfer decision, such as `fromBalance >= amount`, lives in application code. The write itself posts both legs, `MoneyDebited` scoped to the source account and `MoneyCredited` scoped to the destination account, as one atomic `SaveEvents` call using the same four criteria as the read. Either both events commit or neither does; the ledger can never observe a debit without its matching credit. The credit also backlinks to the debit's own event id via `scopes.moneyDebitedId`, so the two legs of one transfer can be found from either side without an invented "transfer id."

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err = client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
	Boundary: "accounts",
	Query: &eventstore.SaveQuery{
		ExpectedPosition: resp.ContextPosition,
		SubsetQuery: &eventstore.Query{
			Criteria: criteria, // the same four criteria used in step 2
		},
	},
	Events: []*eventstore.EventToSave{
		{
			EventId:   "018f2d5e-2003-7000-8000-000000000003",
			EventType: "MoneyDebited",
			Data:      `{"moneyDebitedId":"018f2d5e-2003-7000-8000-000000000003","amount":25,"balanceAfter":75,"scopes.accountOpenedId":"018f2d5e-2001-7000-8000-000000000001"}`,
			Metadata:  `{}`,
		},
		{
			EventId:   "018f2d5e-2004-7000-8000-000000000004",
			EventType: "MoneyCredited",
			Data:      `{"moneyCreditedId":"018f2d5e-2004-7000-8000-000000000004","amount":25,"balanceAfter":25,"scopes.accountOpenedId":"018f2d5e-2002-7000-8000-000000000002","scopes.moneyDebitedId":"018f2d5e-2003-7000-8000-000000000003"}`,
			Metadata:  `{}`,
		},
	},
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.saveEvents({
  boundary: 'accounts',
  query: {
    expectedPosition,
    subsetQuery: { criteria }, // the same four criteria used in step 2
  },
  events: [
    {
      eventId: '018f2d5e-2003-7000-8000-000000000003',
      eventType: 'MoneyDebited',
      data: {
        moneyDebitedId: '018f2d5e-2003-7000-8000-000000000003',
        amount: 25,
        balanceAfter: 75,
        'scopes.accountOpenedId': '018f2d5e-2001-7000-8000-000000000001',
      },
    },
    {
      eventId: '018f2d5e-2004-7000-8000-000000000004',
      eventType: 'MoneyCredited',
      data: {
        moneyCreditedId: '018f2d5e-2004-7000-8000-000000000004',
        amount: 25,
        balanceAfter: 25,
        'scopes.accountOpenedId': '018f2d5e-2002-7000-8000-000000000002',
        'scopes.moneyDebitedId': '018f2d5e-2003-7000-8000-000000000003',
      },
    },
  ],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.saveEvents(Eventstore.SaveEventsRequest.newBuilder()
    .setBoundary("accounts")
    .setQuery(Eventstore.SaveQuery.newBuilder()
        .setExpectedPosition(latest.getContextPosition())
        .setSubsetQuery(subsetQueryFromStep2) // the same four criteria used in step 2
        .build())
    .addEvents(Eventstore.EventToSave.newBuilder()
        .setEventId("018f2d5e-2003-7000-8000-000000000003")
        .setEventType("MoneyDebited")
        .setData("{\"moneyDebitedId\":\"018f2d5e-2003-7000-8000-000000000003\",\"amount\":25,\"balanceAfter\":75,\"scopes.accountOpenedId\":\"018f2d5e-2001-7000-8000-000000000001\"}")
        .build())
    .addEvents(Eventstore.EventToSave.newBuilder()
        .setEventId("018f2d5e-2004-7000-8000-000000000004")
        .setEventType("MoneyCredited")
        .setData("{\"moneyCreditedId\":\"018f2d5e-2004-7000-8000-000000000004\",\"amount\":25,\"balanceAfter\":25,\"scopes.accountOpenedId\":\"018f2d5e-2002-7000-8000-000000000002\",\"scopes.moneyDebitedId\":\"018f2d5e-2003-7000-8000-000000000003\"}")
        .build())
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEvents <<EOF
{
  "boundary": "accounts",
  "query": {
    "expected_position": {"commit_position": 2, "prepare_position": 1},
    "subsetQuery": {
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
  },
  "events": [
    {
      "event_id": "018f2d5e-2003-7000-8000-000000000003",
      "event_type": "MoneyDebited",
      "data": "{\"moneyDebitedId\":\"018f2d5e-2003-7000-8000-000000000003\",\"amount\":25,\"balanceAfter\":75,\"scopes.accountOpenedId\":\"018f2d5e-2001-7000-8000-000000000001\"}",
      "metadata": "{}"
    },
    {
      "event_id": "018f2d5e-2004-7000-8000-000000000004",
      "event_type": "MoneyCredited",
      "data": "{\"moneyCreditedId\":\"018f2d5e-2004-7000-8000-000000000004\",\"amount\":25,\"balanceAfter\":25,\"scopes.accountOpenedId\":\"018f2d5e-2002-7000-8000-000000000002\",\"scopes.moneyDebitedId\":\"018f2d5e-2003-7000-8000-000000000003\"}",
      "metadata": "{}"
    }
  ]
}
EOF
```

  </TabItem>
</Tabs>

Set `expected_position` to the `context_position` observed in step 2, and reuse the exact same four criteria as the read. The save commits only if neither account has moved since. If either account has moved, both legs are rejected together, never just one.

## 4. Handle the conflict

If a second transfer was decided against the same source balance, one of the two saves loses the race. Detect it and retry:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
var conflict *orisun.OptimisticConcurrencyException
if errors.As(err, &conflict) {
	// Concurrency signal. Re-run step 2, re-decide, retry the save.
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
try {
  await client.saveEvents({ /* transfer request */ });
} catch (error) {
  if (error.message.includes('AlreadyExists')) {
    // Concurrency signal. Re-run step 2, re-decide, retry the save.
  } else {
    throw error;
  }
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
try {
    client.saveEvents(transferRequest);
} catch (OptimisticConcurrencyException conflict) {
    // Concurrency signal. Re-run step 2, re-decide, retry the save.
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

This is a concurrency signal. The losing command should:

1. Re-run the four-criteria read in step 2.
2. Read the carried balances from the new latest events.
3. Re-check the invariant. The transfer may no longer be valid.
4. Retry the save with the new `expected_position`.

Reusing the same `event_id`s on retry keeps the command idempotent at the application boundary. See [Idempotency & Retry](./patterns/idempotency-and-retry) for the full pattern, including the retry loop and consumer-side deduplication.

## 5. Index the query field

Steps 2 and 3 filter on both the account root (`eventType = AccountOpened` plus `accountOpenedId`) and later movements (`scopes.accountOpenedId`). Create both indexes once, during deployment. FoundationDB requires these ready covering indexes before the criteria reads or CCC checks will run.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
_, err = client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "accounts",
	Name:     "account_root",
	Fields: []*eventstore.IndexField{{
		JsonKey:   "accountOpenedId",
		ValueType: eventstore.ValueType_TEXT,
	}},
	Conditions: []*eventstore.IndexCondition{{
		Key:      "eventType",
		Operator: "=",
		Value:    "AccountOpened",
	}},
	ConditionCombinator: eventstore.ConditionCombinator_AND,
})

_, err = client.CreateIndex(ctx, &eventstore.CreateIndexRequest{
	Boundary: "accounts",
	Name:     "account_scope",
	Fields: []*eventstore.IndexField{{
		JsonKey:   "scopes.accountOpenedId",
		ValueType: eventstore.ValueType_TEXT,
	}},
})
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
await client.createIndex({
  boundary: 'accounts',
  name: 'account_root',
  fields: [{ jsonKey: 'accountOpenedId', valueType: 'TEXT' }],
  conditions: [{ key: 'eventType', operator: '=', value: 'AccountOpened' }],
  conditionCombinator: 'AND',
});

await client.createIndex({
  boundary: 'accounts',
  name: 'account_scope',
  fields: [{ jsonKey: 'scopes.accountOpenedId', valueType: 'TEXT' }],
});
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
client.createIndex(Eventstore.CreateIndexRequest.newBuilder()
    .setBoundary("accounts")
    .setName("account_root")
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("accountOpenedId")
        .setValueType(Eventstore.ValueType.TEXT)
        .build())
    .addConditions(Eventstore.IndexCondition.newBuilder()
        .setKey("eventType")
        .setOperator("=")
        .setValue("AccountOpened")
        .build())
    .setConditionCombinator(Eventstore.ConditionCombinator.AND)
    .build());

client.createIndex(Eventstore.CreateIndexRequest.newBuilder()
    .setBoundary("accounts")
    .setName("account_scope")
    .addFields(Eventstore.IndexField.newBuilder()
        .setJsonKey("scopes.accountOpenedId")
        .setValueType(Eventstore.ValueType.TEXT)
        .build())
    .build());
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "accounts",
  "name": "account_root",
  "fields": [
    {"json_key": "accountOpenedId", "value_type": "TEXT"}
  ],
  "conditions": [
    {"key": "eventType", "operator": "=", "value": "AccountOpened"}
  ],
  "condition_combinator": "AND"
}
EOF

grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CreateIndex <<EOF
{
  "boundary": "accounts",
  "name": "account_scope",
  "fields": [
    {"json_key": "scopes.accountOpenedId", "value_type": "TEXT"}
  ]
}
EOF
```

  </TabItem>
</Tabs>

Both the CCC consistency check and read queries can now use indexes. See [Indexing](./concepts/indexing) for composite and partial index examples, and [Event Scopes](./patterns/event-scopes#index-scope-keys) for indexing scope keys specifically.

## 6. Project to a read model

A balance read model stays current by subscribing. Catch-up replay reads stored history first, then switches to live JetStream delivery:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
handler := orisun.NewSimpleEventHandler().
	WithOnEvent(func(event *eventstore.Event) error {
		// apply the event, persist side effects, then checkpoint event.Position
		return nil
	}).
	WithOnError(func(err error) {
		log.Printf("subscription stopped: %v", err)
	})

sub, err := client.SubscribeToEvents(ctx, &eventstore.CatchUpSubscribeToEventStoreRequest{
	Boundary:       "accounts",
	SubscriberName: "balance-projector",
	AfterPosition:  &eventstore.Position{CommitPosition: -1, PreparePosition: -1},
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
    subscriberName: 'balance-projector',
    boundary: 'accounts',
    afterPosition: { commitPosition: -1, preparePosition: -1 },
  },
  (event) => {
    // apply the event, persist side effects, then checkpoint event.position
  },
  (error) => console.error('subscription error:', error),
);

// subscription.cancel() to stop
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
EventSubscription sub = client.subscribeToEvents(
    Eventstore.CatchUpSubscribeToEventStoreRequest.newBuilder()
        .setBoundary("accounts")
        .setSubscriberName("balance-projector")
        .setAfterPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(-1).setPreparePosition(-1).build())
        .build(),
    new EventSubscription.EventHandler() {
        public void onEvent(Eventstore.Event event) { /* apply + checkpoint */ }
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
  "subscriber_name": "balance-projector",
  "boundary": "accounts",
  "after_position": {"commit_position": -1, "prepare_position": -1}
}
EOF
```

  </TabItem>
</Tabs>

The stream emits each event with its `position` and `date_created`. A read model that tracks both accounts can pair each `MoneyCredited` with its debit leg via `scopes.moneyDebitedId` to render a transfer history, in addition to updating each account's running balance from `scopes.accountOpenedId`. The projector applies events in order and persists its own checkpoint after each side effect is durable, so a restart resumes from the last processed position. Delivery is at least once, so consumers should deduplicate by `event_id`. See [Delivery Guarantees](./concepts/delivery-guarantees).

## What you built

- A consistency boundary scoped by event content across two accounts, not a fixed stream or an entity table. See [Command Context Consistency](./concepts/command-context-consistency).
- Accounts identified by their own `AccountOpened` event id, with later events referencing it through `scopes.accountOpenedId`. See [Event Scopes](./patterns/event-scopes).
- A double-entry write where a debit and its matching credit commit atomically or not at all.
- An optimistic write that rejects stale context with `ALREADY_EXISTS`.
- An index that keeps the context query and read model fast.
- A live, recoverable projection over the same event log.
