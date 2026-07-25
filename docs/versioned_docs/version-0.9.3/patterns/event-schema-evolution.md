---
title: Event Schema Evolution
description: Evolve event data shapes over time without rewriting history.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Orisun stores the raw `data` JSON for every event and never rewrites history. Old events keep their original shape forever. That immutability is a feature, but it means your application has to cope with multiple versions of an event type existing side by side as your domain evolves.

This page covers the three standard strategies and how each interacts with Orisun's content queries and indexes.

## Strategy 1: Additive changes (preferred when safe)

Only **add** optional fields; never remove or repurpose an existing field. Projectors treat missing fields as defaults.

```json
// v1, written at launch
{"order_id": "ord-1", "amount": 45}

// v2, written later. It adds currency; old events simply lack it.
{"order_id": "ord-1", "amount": 45, "currency": "USD"}
```

This is the safest change. A projector reading `currency` defaults to `"USD"` (or whatever your baseline is) for v1 events. No upcasting, no new event type, no index changes.

Reach for the next two strategies when a change is not purely additive, such as when a field's meaning, type, or structure changes.

## Strategy 2: Versioned event types

Introduce a **new `eventType`** for a breaking change, and leave the old type untouched. Projectors handle both.

```json
{"eventType": "OrderPlaced",     "order_id": "ord-1", "total": "45.00"}
{"eventType": "OrderPlacedV2",   "order_id": "ord-1", "total_cents": 4500, "currency": "USD"}
```

- Old events stay valid under the old contract; new writes use the new type.
- Projectors route on `eventType` and apply the right logic per type.
- Criteria and indexes that target `OrderPlaced` keep matching the old events exactly; target `OrderPlacedV2` separately.

This is the cleanest option for a structural change. The cost is two code paths in consumers until the old type ages out of relevance.

## Strategy 3: Version field with upcasting

Keep one `eventType`, but add a `version` inside `data` and **upcast** old versions to the current shape when you read them.

```json
{"eventType": "PaymentCaptured", "version": 1, "amount": "45.00"}
{"eventType": "PaymentCaptured", "version": 2, "amount_cents": 4500}
```

The projector normalizes before applying:

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
// Upcast every PaymentCaptured to the v2 shape (amount_cents int) before applying.
func normalize(data map[string]any) map[string]any {
	switch v := data["version"]; v {
	case nil, float64(1):
		if s, ok := data["amount"].(string); ok {
			cents, _ := strconv.Atoi(strings.ReplaceAll(s, ".", ""))
			data["amount_cents"] = cents
			data["version"] = 2
		}
	}
	return data
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
// Upcast every PaymentCaptured to the v2 shape (amount_cents number) before applying.
function normalize(data: any) {
  if (!data.version || data.version === 1) {
    if (typeof data.amount === 'string') {
      data.amount_cents = Math.round(parseFloat(data.amount) * 100);
      data.version = 2;
    }
  }
  return data;
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
// Upcast every PaymentCaptured to the v2 shape (amount_cents long) before applying.
// Parse data JSON into a Map<String, Object>, then:
if (!"2".equals(data.get("version"))) {
    String amount = (String) data.get("amount");          // v1: "45.00"
    if (amount != null) {
        data.put("amount_cents", Math.round(Double.parseDouble(amount) * 100));
        data.put("version", 2);
    }
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

Upcasting happens in application code, not at the API. With `grpcurl` you only see the raw stored shapes:

```bash
grpcurl -H "$AUTH" -d '{"boundary":"orders","query":{"criteria":[{"tags":[{"key":"eventType","value":"PaymentCaptured"}]}]},"count":100,"direction":"ASC"}' \
  localhost:5005 orisun.EventStore/GetEvents
```

Both v1 (`"amount":"45.00"`) and v2 (`"amount_cents":4500`) rows come back verbatim.

  </TabItem>
</Tabs>

Keep the upcast logic in one place: a normalizer the projector calls for every event. That gives you a single source of truth for "current shape."

## How evolution interacts with queries and indexes

Criteria queries and [indexes](../concepts/indexing) match JSON keys directly. That has two consequences:

- **A new key does not retroactively match old events.** A criterion on `currency` will not match v1 events that lack it. If you need a unified read across versions, query on a key present in all of them (typically `eventType` or a stable domain id), or upcast before querying.
- **Index stable keys.** Put indexes on fields that do not change across versions (`order_id`, `eventType`). Indexing a field introduced in v2 only speeds up v2+ events.

## Rules of thumb

- **Never silently change a field's meaning.** A field called `amount` that switches from dollars to cents will be misread by every consumer built against the old contract. Use a versioned type or a version field.
- **Prefer additive changes.** They require no upcasting and no new event type.
- **Version the type for structural breaks.** Two explicit types beat one ambiguous one.
- **Centralize upcasting.** One normalizer, applied at every read boundary.

## Summary

| Change kind | Strategy | Rewrites history? |
| --- | --- | --- |
| Add an optional field | Additive (default the new field) | No |
| Restructure or repurpose fields | Versioned `eventType` (`X` → `XV2`) | No |
| Same type, new shape | `version` field + upcast on read | No |

All three keep the log immutable; the difference is where the compatibility work lives: in the writer, in the type name, or in the reader.
