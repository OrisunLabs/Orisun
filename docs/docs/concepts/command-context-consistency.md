---
title: Command Context Consistency
description: Define and atomically validate every event query a command depends on.
---

Command Context Consistency, or CCC, is Orisun's optimistic consistency model. A command reads the event facts needed for a decision, records the query and latest matching position for each read, then saves only if every observation is still current.

Traditional event stores often make one stream the concurrency boundary. Orisun lets each command declare the content-defined contexts its own invariants require.

## The model

A consistency observation has exactly two parts:

```json
{
  "query": {
    "criteria": [
      {"tags": [{"key": "scopes.warehouseId", "value": "warehouse-1"}]},
      {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}
    ]
  },
  "position": {"commit_position": 42, "prepare_position": 3}
}
```

Tags inside one criterion are combined with **AND**. Criteria inside one query are combined with **OR**. The position belongs to the complete query, not to an individual criterion: it is the latest position matched by any criterion in that query.

`SaveEventsV2` accepts zero or more observations. Before inserting anything, Orisun re-runs every query and compares its latest matching position with the supplied position. All checks and all inserts happen atomically. If one observation is stale, no event is saved and the RPC returns `ALREADY_EXISTS`.

An empty `consistency` list is an unconditional atomic append. Use `{-1, -1}` when a query matched no event and must remain empty.

## Command flow

1. **Read** each context needed by the command.
2. **Decide** business rules in application code.
3. **Record** the new events with the observations produced by those reads.
4. **Retry** from the read if Orisun returns `ALREADY_EXISTS`.

## One OR query, one position

Suppose dispatching a shipment depends on either warehouse activity or shipment activity. Read both alternatives in one `GetLatestByCriteria` call:

```json
{
  "boundary": "shipping",
  "criteria": [
    {"tags": [{"key": "scopes.warehouseId", "value": "warehouse-1"}]},
    {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}
  ]
}
```

The unchanged response contains the latest result for each criterion and one `context_position` for the complete read:

```json
{
  "results": [
    {"criterion": {"tags": [{"key": "scopes.warehouseId", "value": "warehouse-1"}]}, "event": {}},
    {"criterion": {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}, "event": {}}
  ],
  "context_position": {"commit_position": 42, "prepare_position": 3}
}
```

Construct the `SaveEventsV2` observation by pairing the exact criteria sent in the request, wrapped as one `Query`, with the returned `context_position`. Do not split it into one position per criterion: the read observed the complete OR query as one context.

## Multiple independently read contexts

Some commands use different read shapes. A dispatch might use `GetLatestByCriteria` for current warehouse and shipment state, then `GetEvents` to load the carrier's full suspension history. Each complete read produces its own observation:

```json
{
  "boundary": "shipping",
  "events": [
    {
      "event_id": "018f2d5e-3001-7000-8000-000000000001",
      "event_type": "ShipmentDispatched",
      "data": "{\"shipmentId\":\"shipment-9\",\"warehouseId\":\"warehouse-1\",\"carrierId\":\"carrier-2\"}",
      "metadata": "{}"
    }
  ],
  "consistency": [
    {
      "query": {
        "criteria": [
          {"tags": [{"key": "scopes.warehouseId", "value": "warehouse-1"}]},
          {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}
        ]
      },
      "position": {"commit_position": 42, "prepare_position": 3}
    },
    {
      "query": {
        "criteria": [
          {"tags": [
            {"key": "eventType", "value": "CarrierSuspended"},
            {"key": "carrierId", "value": "carrier-2"}
          ]},
          {"tags": [
            {"key": "eventType", "value": "CarrierReinstated"},
            {"key": "carrierId", "value": "carrier-2"}
          ]}
        ]
      },
      "position": {"commit_position": 35, "prepare_position": 0}
    }
  ]
}
```

The reads do not need to share one snapshot. Each position is tied to its own complete query, so a matching event committed after either read makes that observation stale. `SaveEventsV2` validates both in the write transaction before appending.

When deriving an observation from a complete `GetEvents` read, preserve the exact query and use the latest matching event's position. Use `{-1, -1}` if the complete read returned no match. A truncated page is not a complete context read and must not be used as an observation.

## Before and after

The deprecated API could protect only one query:

```json
{
  "boundary": "shipping",
  "query": {
    "expected_position": {"commit_position": 42, "prepare_position": 3},
    "subsetQuery": {
      "criteria": [
        {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}
      ]
    }
  },
  "events": [{}]
}
```

The replacement makes the invariant explicit and supports every context the command read:

```json
{
  "boundary": "shipping",
  "events": [{}],
  "consistency": [
    {
      "query": {
        "criteria": [
          {"tags": [{"key": "scopes.shipmentId", "value": "shipment-9"}]}
        ]
      },
      "position": {"commit_position": 42, "prepare_position": 3}
    }
  ]
}
```

`SaveEvents` and `SaveQuery` remain wire-compatible but are deprecated. The server translates a V1 request into one V2 observation and executes the same implementation. New applications should use `SaveEventsV2`.

## Conflict behavior

When any observed query has changed, Orisun returns gRPC `ALREADY_EXISTS`. This is an expected concurrency signal, not a server failure. The application should re-read all contexts needed by the decision, rebuild its model, re-run business validation, and retry with fresh observations only if the command remains valid.

Orisun validates equality queries and positions; it does not perform domain decoding or business validation. Those remain the command handler's responsibility.

## Design guidance

- Keep each query as narrow as the command's invariant allows.
- Preserve query-level observations; never attach separate positions to criteria from one OR query.
- When `GetLatestByCriteria` informs a command, pair its exact request criteria with its returned `context_position`.
- Use multiple observations when a command genuinely made multiple complete reads.
- Create indexes for fields used by high-volume contexts. FoundationDB requires ready covering indexes for all criteria reads and checks.
- Treat `ALREADY_EXISTS` as a signal to re-read and decide again.

If you are comparing terminology, see [CCC and DCB terminology](./dynamic-consistency-boundaries). Orisun product APIs and guidance use CCC.
