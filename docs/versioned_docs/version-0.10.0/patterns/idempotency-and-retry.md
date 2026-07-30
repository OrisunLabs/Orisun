---
title: Idempotency & Retry
description: Make commands safe to retry and consumers safe to reprocess.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Orisun gives you two distinct idempotency problems to solve, and a mechanism for each:

- **Write side:** a command may need to be retried (network blip, contention, or a lost response). Retrying must not double-apply the business decision.
- **Read side:** delivery is [at least once](../concepts/delivery-guarantees#at-least-once-delivery), so a projector can see the same event more than once. Reprocessing must not double-apply a side effect.

## Write side: the CCC check is your idempotency guard

The primary idempotency mechanism is the Command Context Consistency `expected_position`, not the `event_id`:

1. Read the command context (`GetLatestByCriteria` or `GetEvents`) and note the position.
2. Save with that position as `expected_position` and a `subsetQuery` for the context.
3. If the save **committed** and you retry with the *same* `expected_position`, Orisun returns `ALREADY_EXISTS` because the context already advanced, rather than writing a duplicate.

So `ALREADY_EXISTS` after a retry means *"something already moved this context."* Very often, that was your own first attempt.

:::note
The store does **not** deduplicate by `event_id`. There is no unique constraint on `event_id` (the primary key is the per-boundary `global_id`). A stable `event_id` is for *detection* and *consumer dedup*; the CCC check is what prevents duplicate writes.
:::

### Use a command-stable event_id

Assign the `event_id` when the command is first accepted, then reuse it on every retry of that command. A UUIDv7 works well when it is generated once and carried with the command; what breaks idempotency is generating a fresh value on every attempt. A retried command should carry the same `event_id` as the original so you and your projectors can recognize it.

### Retry loop

On `ALREADY_EXISTS`, re-read the context and re-decide because the invariant may no longer hold. Loop until the save commits or the decision is no longer valid:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
// Stable for this command. Do not call uuid.NewString() on each attempt.
eventID := "018f2d5e-00a1-7000-8000-0000000000a1"
accountOpenedID := "018f2d5e-2001-7000-8000-000000000001"
accountCriteria := []*eventstore.Criterion{
	{Tags: []*eventstore.Tag{
		{Key: "eventType", Value: "AccountOpened"},
		{Key: "accountOpenedId", Value: accountOpenedID},
	}},
	{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: accountOpenedID}}},
}

for {
	latest, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
		Boundary: "accounts",
		Criteria: accountCriteria,
	})
	if err != nil {
		return err
	}

	balance := readBalance(latest) // application reads carried state
	if balance < amount {
		return ErrInsufficientFunds // no longer valid; stop
	}

	_, err = client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
		Boundary: "accounts",
		Query: &eventstore.SaveQuery{
			ExpectedPosition: latest.ContextPosition,
			SubsetQuery: &eventstore.Query{
				Criteria: accountCriteria,
			},
		},
		Events: []*eventstore.EventToSave{{
			EventId:   eventID,
			EventType: "MoneyDebited",
			Data:      `{"moneyDebitedId":"` + eventID + `","amount":40,"balanceAfter":` + bal(balance-amount) + `,"scopes.accountOpenedId":"` + accountOpenedID + `"}`,
		}},
	})
	if err == nil {
		return nil
	}

	var conflict *orisun.OptimisticConcurrencyException
	if !errors.As(err, &conflict) {
		return err // a real failure, not a concurrency signal
	}
	// Context changed between read and write, so loop re-reads and re-decides.
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
const eventId = '018f2d5e-00a1-7000-8000-0000000000a1'; // stable per command
const accountOpenedId = '018f2d5e-2001-7000-8000-000000000001';
const accountCriteria = [
  { tags: [
    { key: 'eventType', value: 'AccountOpened' },
    { key: 'accountOpenedId', value: accountOpenedId },
  ] },
  { tags: [{ key: 'scopes.accountOpenedId', value: accountOpenedId }] },
];

for (;;) {
  const latest = await client.getLatestByCriteria({
    boundary: 'accounts',
    criteria: accountCriteria,
  });

  const balance = latest.results[1].event?.data.balanceAfter
    ?? latest.results[0].event?.data.balance
    ?? 0;
  if (balance < amount) throw new Error('insufficient funds');

  try {
    await client.saveEvents({
      boundary: 'accounts',
      query: {
        expectedPosition: latest.contextPosition,
        subsetQuery: { criteria: accountCriteria },
      },
      events: [{
        eventId,
        eventType: 'MoneyDebited',
        data: {
          moneyDebitedId: eventId,
          amount,
          balanceAfter: balance - amount,
          'scopes.accountOpenedId': accountOpenedId,
        },
      }],
    });
    return;
  } catch (error) {
    if (!error.message.includes('AlreadyExists')) throw error;
    // Context changed between read and write, so loop re-reads and re-decides.
  }
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
String eventId = "018f2d5e-00a1-7000-8000-0000000000a1"; // stable per command
String accountOpenedId = "018f2d5e-2001-7000-8000-000000000001";
Eventstore.Query accountQuery = Eventstore.Query.newBuilder()
    .addCriteria(Eventstore.Criterion.newBuilder()
        .addTags(Eventstore.Tag.newBuilder().setKey("eventType").setValue("AccountOpened").build())
        .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue(accountOpenedId).build())
        .build())
    .addCriteria(Eventstore.Criterion.newBuilder()
        .addTags(Eventstore.Tag.newBuilder().setKey("scopes.accountOpenedId").setValue(accountOpenedId).build())
        .build())
    .build();

while (true) {
    Eventstore.GetLatestByCriteriaResponse latest = client.getLatestByCriteria(
        Eventstore.GetLatestByCriteriaRequest.newBuilder()
            .setBoundary("accounts")
            .addAllCriteria(accountQuery.getCriteriaList())
            .build());

    long balance = readBalance(latest); // application reads carried state
    if (balance < amount) throw new IllegalStateException("insufficient funds");

    try {
        client.saveEvents(Eventstore.SaveEventsRequest.newBuilder()
            .setBoundary("accounts")
            .setQuery(Eventstore.SaveQuery.newBuilder()
                .setExpectedPosition(latest.getContextPosition())
                .setSubsetQuery(accountQuery)
                .build())
            .addEvents(Eventstore.EventToSave.newBuilder()
                .setEventId(eventId)
                .setEventType("MoneyDebited")
                .setData(debitJson(eventId, accountOpenedId, amount, balance - amount))
                .build())
            .build());
        return;
    } catch (OptimisticConcurrencyException conflict) {
        // Context changed between read and write, so loop re-reads and re-decides.
    }
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

`grpcurl` is not suited to retry loops because it makes one call. Use it to reproduce a single conflict:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEvents <<EOF
{
  "boundary": "accounts",
  "query": {
    "expected_position": {"commit_position": 2, "prepare_position": 1},
    "subsetQuery": {"criteria": [
      {"tags": [
        {"key": "eventType", "value": "AccountOpened"},
        {"key": "accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}
      ]},
      {"tags": [{"key": "scopes.accountOpenedId", "value": "018f2d5e-2001-7000-8000-000000000001"}]}
    ]}
  },
  "events": [{
    "event_id": "018f2d5e-00a1-7000-8000-0000000000a1",
    "event_type": "MoneyDebited",
    "data": "{\"moneyDebitedId\":\"018f2d5e-00a1-7000-8000-0000000000a1\",\"amount\":40,\"balanceAfter\":60,\"scopes.accountOpenedId\":\"018f2d5e-2001-7000-8000-000000000001\"}"
  }]
}
EOF
```

  </TabItem>
</Tabs>

### Ambiguous failures: "maybe it committed"

A timeout *after* the server received the save but *before* you got the response is ambiguous because the command may have committed. Treat it like a conflict: re-read the context. If the carried state already reflects your decision, your first attempt committed; do not apply it again. A command-stable `event_id` makes this check recognizable downstream.

## Read side: deduplicate by event_id in the projector

Because delivery is at least once, a projector must treat `apply(event)` as idempotent. Two common approaches:

- **Idempotent writes:** make the side effect a keyed upsert keyed by `event_id`, so applying the same event twice converges. Simplest.
- **Processed-event table:** record each processed `event_id`; skip events already seen. Needed when the side effect is not naturally idempotent (e.g. appending to an external ledger).

Persist the projector checkpoint **after** the side effect is durable, so a restart resumes from the last fully-applied event rather than re-emitting it. See [Delivery Guarantees](../concepts/delivery-guarantees).

## Summary

| Concern | Mechanism |
| --- | --- |
| Don't write a duplicate on retry | CCC `expected_position` → `ALREADY_EXISTS` |
| Recognize a retried command | Command-stable `event_id` |
| Don't double-apply on redelivery | Consumer dedup by `event_id` |
| Recover from a lost response | Re-read the context before retrying |
