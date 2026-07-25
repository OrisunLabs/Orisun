---
title: Observability
description: Health, tracing, metrics, logging, and profiling signals Orisun emits and how to enable them.
---

Orisun exposes standard gRPC health, distributed traces, application and RPC
metrics, structured logs, and on-demand profiles.

## Health and readiness

Orisun implements the standard `grpc.health.v1.Health` service. Health
`Check`, `List`, and `Watch` calls do not require Orisun credentials, so
orchestrators can probe a node before application authentication is available.

Check the whole server:

```bash
grpcurl -plaintext \
  -d '{"service":""}' \
  localhost:5005 grpc.health.v1.Health/Check
```

Or check either registered application service:

```bash
grpcurl -plaintext \
  -d '{"service":"orisun.EventStore"}' \
  localhost:5005 grpc.health.v1.Health/Check

grpcurl -plaintext \
  -d '{"service":"orisun.Admin"}' \
  localhost:5005 grpc.health.v1.Health/Check
```

The response is `NOT_SERVING` while the gRPC server is being prepared and
`SERVING` after backend initialization, admin catalog bootstrap, service
registration, and listener creation have succeeded. Cancellation of the
server context changes all registered statuses back to `NOT_SERVING`.

After startup, Orisun probes JetStream and durable storage every five seconds.
The JetStream probe makes a control-plane round trip; the storage probe performs
a read-only query against the admin boundary. A failed probe changes the whole
server plus the EventStore and Admin service statuses to `NOT_SERVING`.
Readiness returns to `SERVING` automatically when both probes recover.

Health does not yet evaluate individual boundary lifecycle failures, publisher
lag, or publisher ownership; monitor those signals separately.

Kubernetes can use its native gRPC probe:

```yaml
readinessProbe:
  grpc:
    port: 5005
  periodSeconds: 5
```

## OpenTelemetry

Orisun emits OpenTelemetry traces and metrics over OTLP gRPC. Point the
endpoint at an OpenTelemetry Collector with both trace and metric pipelines;
the collector can route each signal to Prometheus, Tempo, or another backend.

| Variable | Default | Description |
| --- | --- | --- |
| `ORISUN_OTEL_ENABLED` | `true` | Enable OpenTelemetry traces and metrics. |
| `ORISUN_OTEL_ENDPOINT` | `localhost:4317` | OTLP gRPC collector endpoint. |
| `ORISUN_OTEL_SERVICE_NAME` | `orisun` | Service name attached to exported telemetry. |

Metrics are exported every 15 seconds.

RPC metrics:

| Metric | Meaning |
| --- | --- |
| `orisun.rpc.server.requests` | Completed gRPC calls. |
| `orisun.rpc.server.active_requests` | Calls currently in flight. |
| `rpc.server.call.duration` | Call duration in seconds using the OpenTelemetry RPC histogram buckets. |

These metrics include `rpc.system.name`, `rpc.method`, `orisun.rpc.type`, and
the final `rpc.response.status_code`. Failed calls also include `error.type`.
These attributes let dashboards separate EventStore, Admin, health, unary, and
streaming traffic without parsing spans.

Event-store write metrics:

| Metric | Meaning |
| --- | --- |
| `orisun.eventstore.commits` | Event batches committed successfully. |
| `orisun.eventstore.events` | Events committed successfully. |
| `orisun.eventstore.payload.size` | Uncompressed event data and metadata bytes committed successfully. |
| `orisun.eventstore.commit.duration` | Durable backend commit-attempt duration in seconds, including failed attempts. |
| `orisun.ccc.conflicts` | Writes rejected because their Command Context Consistency context changed. |

Write metrics include `orisun.boundary.name`. Commit duration also includes
`orisun.eventstore.commit.status`; failures add the bounded `error.type`
status. CCC conflicts include a bounded `orisun.ccc.criterion_shape` value:
`unscoped`, `position_only`, `empty_criterion`,
`single_criterion_single_tag`, `single_criterion_multiple_tags`, or
`multiple_criteria`. Criterion keys and values and raw error messages are
never metric attributes.

The commit, event, and payload counters advance only after durable storage
accepts the batch. Payload size is the byte length of the normalized event
data and metadata JSON; it excludes transport framing, event IDs, and event
type fields outside the data document.

Orisun intentionally does not expose a Prometheus `/metrics` endpoint. Use the
collector's Prometheus exporter or remote-write exporter when Prometheus is the
metrics backend.

## Logging

Logs are structured and leveled. Set the level with `ORISUN_LOGGING_LEVEL` (`DEBUG`, `INFO`, `WARN`, `ERROR`; default `INFO`).

Operationally useful log lines include:

- effective `GOMAXPROCS`, `GOMEMLIMIT`, and `GOGC` at startup,
- admin boundary bootstrap and catalog replay,
- boundary provisioning failures and independent retry attempts,
- publisher boundary-lock acquisition and contention in clustered deployments,
- publisher checkpoint progress and any publish errors.

Use `DEBUG` to trace authentication, subscription handover, and per-call detail; keep `INFO` or higher in production.

## Profiling

The Go `pprof` server is available for CPU, heap, and goroutine profiling when you need to investigate a performance issue.

| Variable | Default | Description |
| --- | --- | --- |
| `ORISUN_PPROF_ENABLED` | `false` | Enable the pprof HTTP server. |
| `ORISUN_PPROF_PORT` | `6060` | pprof listen port. |

```bash
go tool pprof http://localhost:6060/debug/pprof/heap
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

Leave pprof disabled in production unless you are actively profiling, and never expose its port publicly.

## What to watch

- **Boundary readiness** is the set of `PROVISIONING` or `FAILED` entries from
  `Admin/ListBoundaries`. Alert on definitions that stay non-active and include
  `last_error`, `placement`, and `status_position` in diagnostics.
- **Publisher lag** measures the gap between committed and published positions. Investigate with the [Troubleshooting](./troubleshooting#publisher-lag) steps.
- **Catch-up vs live** tells you whether subscribers are repeatedly falling out of live delivery. If they are, the JetStream retention window may be too small for their pace. See [Delivery Guarantees](../concepts/delivery-guarantees#jetstream-retention-is-in-memory).
- **Consistency conflicts** show a high `ALREADY_EXISTS` rate, which is a domain hotspot, not an error. Narrow the consistency context or add an [index](../concepts/indexing).
