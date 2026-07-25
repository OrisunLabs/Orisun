---
title: Projection Rebuild
description: Rebuild a read model from the durable event log without losing events.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The durable event log in PostgreSQL, YugabyteDB, SQLite, or FoundationDB is the source of truth; every read model is derived from it. That means any projection can be rebuilt from scratch at any time, including after a bug fix, a schema change in the read model, or a corrupted store. You do not depend on JetStream retention for correctness; you depend on the log. See [Delivery Guarantees](../concepts/delivery-guarantees).

## When to rebuild

- you fixed a bug in the projector and need the corrected read model,
- you added a new read model to an existing boundary,
- a projection store was corrupted or lost,
- you changed the shape of the read model and want a clean recompute.

## The rebuild loop

A rebuild is a catch-up subscription that starts from the beginning position and re-applies every event to a fresh target:

1. **Stop or divert** the live projector so it is not writing while you rebuild.
2. **Reset the target** by truncating or dropping the read model so re-applied events rebuild cleanly.
3. **Subscribe from the start** with `after_position` `{0, 0}`. This is the beginning cursor for reads and subscriptions; see [Positions](../concepts/positions#empty-and-beginning-positions).
4. **Apply idempotently** because delivery is at least once, and a rebuild may revisit events. Deduplicate by `event_id`.
5. **Checkpoint after each side effect is durable**, then transition to live delivery. The subscription switches to JetStream once catch-up drains, so the rebuilt model continues without a gap.

## Example

Use a distinct `subscriber_name` for the rebuild (or stop the old one first) so the checkpoint does not collide with the live projector.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
// 1. Reset the target read model out of band (e.g. TRUNCATE, drop+recreate).
// 2. Rebuild from the beginning, applying each event idempotently.

handler := orisun.NewSimpleEventHandler().
	WithOnEvent(func(event *eventstore.Event) error {
		// Apply idempotently: upsert keyed by event.eventId, or check a
		// processed-events table. Redelivery must not double-apply.
		if alreadyProcessed(event.EventId) {
			return nil
		}
		if err := applyToReadModel(event); err != nil {
			return err
		}
		// Checkpoint AFTER the side effect is durable.
		return saveCheckpoint(event.Position)
	}).
	WithOnError(func(err error) {
		log.Printf("rebuild stopped: %v", err)
	})

sub, err := client.SubscribeToEvents(ctx, &eventstore.CatchUpSubscribeToEventStoreRequest{
	Boundary:       "accounts",
	SubscriberName: "balance-projector-rebuild",
	AfterPosition:  &eventstore.Position{CommitPosition: 0, PreparePosition: 0},
}, handler)
if err != nil {
	return err
}
defer sub.Close()
// Catch-up replays all history, then the stream switches to live delivery.
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
// 1. Reset the target read model out of band (e.g. TRUNCATE, drop+recreate).
// 2. Rebuild from the beginning, applying each event idempotently.

const subscription = client.subscribeToEvents(
  {
    subscriberName: 'balance-projector-rebuild',
    boundary: 'accounts',
    afterPosition: { commitPosition: 0, preparePosition: 0 },
  },
  async (event) => {
    if (alreadyProcessed(event.eventId)) return;
    await applyToReadModel(event); // upsert keyed by event.eventId
    await saveCheckpoint(event.position); // AFTER the side effect is durable
  },
  (error) => console.error('rebuild stopped:', error),
);

// Catch-up replays all history, then the stream switches to live delivery.
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
// 1. Reset the target read model out of band (e.g. TRUNCATE, drop+recreate).
// 2. Rebuild from the beginning, applying each event idempotently.

EventSubscription sub = client.subscribeToEvents(
    Eventstore.CatchUpSubscribeToEventStoreRequest.newBuilder()
        .setBoundary("accounts")
        .setSubscriberName("balance-projector-rebuild")
        .setAfterPosition(Eventstore.Position.newBuilder()
            .setCommitPosition(0).setPreparePosition(0).build())
        .build(),
    new EventSubscription.EventHandler() {
        public void onEvent(Eventstore.Event event) {
            if (alreadyProcessed(event.getEventId())) return;
            applyToReadModel(event); // upsert keyed by event id
            saveCheckpoint(event.getPosition()); // AFTER the side effect is durable
        }
        public void onError(Throwable error) { error.printStackTrace(); }
        public void onCompleted() {}
    });

// Catch-up replays all history, then the stream switches to live delivery.
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

`grpcurl` opens a streaming subscription you can watch, but a real rebuild needs a program that applies and checkpoints. Use it to verify the replay starts from the beginning:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.EventStore/CatchUpSubscribeToEvents <<EOF
{
  "subscriber_name": "balance-projector-rebuild",
  "boundary": "accounts",
  "after_position": {"commit_position": 0, "prepare_position": 0}
}
EOF
```

  </TabItem>
</Tabs>

## Cut over

Once the rebuild's catch-up has drained and the subscriber is live, switch readers to the rebuilt model. If you used a separate `subscriber_name`, redirect reads to the new store and retire the old projector. If you reset in place, the live projector simply continues from its new checkpoint.

## Speed up large rebuilds

- **Index the filter fields.** A filtered rebuild (for example only `OrderPlaced` events) scans the boundary table without an index. [Create a partial index](../concepts/indexing#partial-index) on the filtered field first.
- **Rebuild in parallel by boundary.** Boundaries are independent, so rebuild each on its own subscriber.
- **Page instead of stream** if you want bounded concurrency: use `GetEvents` with increasing `from_position` and apply page by page, then start the live subscription from the last position.

## Summary

| Step | Why |
| --- | --- |
| Reset the target first | So re-applied events rebuild, not append |
| Start from `{0, 0}` | Beginning cursor that replays the whole log |
| Apply idempotently | Redelivery must not double-apply |
| Checkpoint after durability | A restart resumes, never re-emits |
| Index filter fields | Avoid full-table scans on filtered rebuilds |
