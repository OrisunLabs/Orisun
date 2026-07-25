---
title: Observability
description: Tracing, logging, and profiling signals Orisun emits and how to enable them.
---

Orisun exposes three operational signals: distributed traces, structured logs, and on-demand profiles.

## Tracing

Orisun emits OpenTelemetry **traces** and exports them over OTLP gRPC. This is the primary signal for understanding request flow and latency across the save, query, and publish paths.

| Variable | Default | Description |
| --- | --- | --- |
| `ORISUN_OTEL_ENABLED` | `true` | Enable OpenTelemetry tracing. |
| `ORISUN_OTEL_ENDPOINT` | `localhost:4317` | OTLP gRPC collector endpoint. |
| `ORISUN_OTEL_SERVICE_NAME` | `orisun` | Service name attached to exported spans. |

Point `ORISUN_OTEL_ENDPOINT` at any OTLP-compatible collector (the OpenTelemetry Collector, Tempo, Jaeger with OTLP, Honeycomb, etc.). Traces are tagged with the service name so multiple nodes are distinguishable.

:::note
Orisun currently exports traces, not a built-in metrics endpoint. For request rates and latencies, derive them from spans in your tracing backend, or scrape the Go runtime via `pprof`. There is no Prometheus `/metrics` endpoint to scrape.
:::

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
