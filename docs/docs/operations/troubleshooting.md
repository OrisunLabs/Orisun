---
title: Troubleshooting
description: Diagnose startup, API, consistency, and subscription issues.
---

Start with the symptom table, then use the focused sections below.

| Symptom | Check |
| --- | --- |
| Cannot connect | PostgreSQL host and port, gRPC port, firewall, Docker networking. |
| `ALREADY_EXISTS` | Expected CCC conflict; re-query context and retry only if the command is still valid. |
| Boundary stays `PROVISIONING` or becomes `FAILED` | Inspect `Admin/GetBoundary.last_error`, placement, backend connectivity, and provisioning retry logs. |
| Slow criteria queries | Missing JSON field indexes for the selected backend. |
| Subscription lag | Backend query cost, handler/checkpoint progress, NATS health, idle threshold, and relay lease ownership. |
| Duplicate delivery | Expected when resuming from an application checkpoint that trails delivered events; deduplicate by `event_id`. |
| Cluster instability | NATS quorum, routes, unique ports, persistent store directories. |

## gRPC Reflection

If `grpcurl localhost:5005 list` fails, check:

- `ORISUN_GRPC_PORT`
- container port mappings
- `ORISUN_GRPC_ENABLE_REFLECTION`
- authentication headers

Use the default Basic auth header for examples:

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
grpcurl -plaintext -H "$AUTH" localhost:5005 list
```

## Boundary Not Found

Requests to unknown or not-yet-installed boundaries are rejected. Query the
catalog first:

```bash
grpcurl -plaintext -H "$AUTH" localhost:5005 orisun.Admin/ListBoundaries
grpcurl -plaintext -H "$AUTH" \
  -d '{"name":"orders"}' \
  localhost:5005 orisun.Admin/GetBoundary
```

- If the definition is absent, call `CreateBoundary`; set
  the matching placement for storage that already uses the current format.
- If it is `PROVISIONING`, wait; the definition event committed but the local
  runtime is not ready yet.
- If it is `FAILED`, inspect `last_error`. Verify that backend and namespace
  match the running backend: PostgreSQL uses a schema, SQLite requires the
  boundary name.
- Do not call create again for a failed definition. Its immutable name
  already exists and the server is retrying it independently.

If startup reports an unsupported storage version or an existing admin store
without its catalog, stop and verify the restored backup and source version.
Only `0.13.0` upgrades in place. Older and unversioned stores require export
with their matching binary and import into fresh storage; see the
[storage upgrade policy](./upgrading-event-envelope). Do not change version markers
or assume that restoring application tables restores the admin catalog.

For SQLite, verify both `{boundary}.db` and `{boundary}_metadata.db` are in
`ORISUN_SQLITE_DIR` before registration.

## Subscription Lag

Boundary notifications are hints. If lag persists:

1. Check the backend is reachable and the subscription query can read its stable ascending prefix.
2. Check handler latency and application/projector checkpoint progress.
3. Check `ORISUN_SUBSCRIPTION_IDLE_THRESHOLD` and backend read-pool contention.
4. Check NATS health and the notification relay lease owner for latency improvements.

Publisher checkpoints and legacy event-stream retention no longer control
subscription delivery. Healthy NATS is required for live delivery. Idle watchdog hints recover missed
relay signals after the configured threshold.

## Duplicate Events

Orisun delivery is at least once. Consumers should be idempotent and deduplicate using stable event identifiers.

## Slow criteria queries

Criteria queries read JSON fields from event `data`. On PostgreSQL and SQLite,
create indexes for high-volume keys used in command contexts,
`GetLatestByCriteria`, or projector filters. See
[Indexing](../concepts/indexing).
