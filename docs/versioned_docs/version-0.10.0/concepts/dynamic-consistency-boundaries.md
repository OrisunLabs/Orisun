---
title: CCC and DCB Terminology
description: A comparison for readers evaluating Orisun's CCC model alongside Dynamic Consistency Boundaries.
---

Orisun implements **Command Context Consistency (CCC)**. Applications using
Orisun need only CCC: query the events behind a decision, remember the observed
context position, and save only while that same context remains current.

[Dynamic Consistency Boundaries](https://dcb.events/specification/) (DCB)
describes a related event-store pattern. The shared idea is content-scoped
optimistic consistency, but Orisun does not claim DCB specification compliance
and does not expose a separate DCB mode or API.

## Concept mapping

| DCB concept | Orisun CCC concept |
| --- | --- |
| Event type | `event_type` on save; queryable as `eventType` in stored event data |
| Tags | Queryable JSON fields in event `data` |
| Query | `Query.criteria` used by `GetEvents` or `GetLatestByCriteria` |
| Append condition | `SaveEvents.query.expected_position` plus `subsetQuery` |
| Condition failure | `ALREADY_EXISTS` |

This mapping is terminology for comparison, not a second implementation path.
Use the [CCC workflow](./command-context-consistency) and the
[EventStore API](../api/eventstore) when building an Orisun application.

## Important differences

- **Condition semantics.** A DCB append condition rejects when its query matches
  an event after the optional `after` position. Orisun instead requires the
  latest event matching `subsetQuery` to equal `expected_position`. A position
  later than the latest match is valid in DCB but fails Orisun's equality check.
- **Query shape.** DCB query items have first-class event-type and tag filters.
  Orisun criteria match JSON fields; event type is queried through the canonical
  `eventType` field.
- **Match-all queries.** The DCB specification requires a
  `failIfEventsMatch` query and permits that query to match all events. Orisun
  has no portable match-all CCC condition. Do not use empty criteria as a
  match-all query.
- **Position representation.** DCB requires unique, monotonically increasing
  positions but does not prescribe their representation. Orisun represents a
  position as `(commit_position, prepare_position)` and applications should
  treat the pair as an opaque ordering token.
- **Boundary terminology.** An Orisun boundary is a catalogued storage
  partition. A DCB consistency boundary is the event subset selected for a
  decision.

These differences are why the Orisun documentation describes the feature as
CCC rather than advertising DCB compatibility.
