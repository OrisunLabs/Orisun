---
id: getting-started
title: Getting Started
description: Choose a backend, run Orisun, and verify the gRPC API.
slug: /getting-started
---

Orisun is an event database for decisions that must stay correct as facts change: preserve the event history a command depends on, commit only when its declared context is still current, and deliver matching committed events sequentially within each subscription without running a separate broker.

These pages describe the breaking changes after `0.13.0`. When reading **Next**,
build the checked-out source for its notification and storage behavior. Select
**0.13.0** in the version menu when running that release; it already supports
`SaveEventsV2`.

You can run Orisun as a release binary, a Docker image, or an embedded Go package. Docker Compose is convenient for trying the full stack, but production deployments can run the binary directly under systemd, Nomad, Kubernetes, Fly, Render, or any process supervisor.

This guide starts a standalone server. If you want Orisun inside a Go process, go to [Go Embedding](./embedding/go).

## Before you start

You need:

- an Orisun release binary or Docker
- `grpcurl` for the verification command
- a boundary name for your application events
- a separate boundary for Orisun admin state

The examples use:

| Boundary | Purpose |
| --- | --- |
| `orders` | application events |
| `orisun_admin` | users, credentials, and admin projections |

## Fastest local path

Use SQLite first when you want the shortest feedback loop. To try the upcoming
release, build the checked-out source:

1. Build `orisun-sqlite` with the commands under [Install a binary](#install-a-binary).
2. Start it with the [SQLite binary example](#run-sqlite-from-a-binary).
3. Verify the server with [grpcurl](#verify-the-api).
4. Create the `orders` boundary with [Create the application boundary](#create-the-application-boundary).
5. Save the first event with [Save your first event](#save-your-first-event).

You can move to PostgreSQL later without changing the EventStore API.

## Choose how to run

| Runtime | Use when | Start here |
| --- | --- | --- |
| Release binary | You want to deploy Orisun directly as a server process. | [Install a binary](#install-a-binary) |
| Docker image | You want a packaged container or Docker Compose for local setup. | [Run SQLite with Docker](#run-sqlite-with-docker) |
| Embedded Go package | You want Orisun inside your service process. | [Go Embedding](./embedding/go) |

:::note
Release downloads and channel tags contain published releases, not necessarily
the source described by Next. Use a source build for Next and pin a versioned
image tag for a deployed release.
:::

## Choose a backend

| Backend | Best for | Multi-node | Binary | Docker Hub image | GHCR image |
| --- | --- | --- | --- | --- | --- |
| SQLite | Embedded apps, edge services, development, single-node production | No | `orisun-sqlite` | `orisunlabs/orisun:sqlite` | `ghcr.io/orisunlabs/orisun:sqlite` |
| PostgreSQL | Clustered deployments, larger datasets, shared database platforms | Yes | `orisun-pg` | `orisunlabs/orisun:pg` | `ghcr.io/orisunlabs/orisun:pg` |

Both backends expose the same EventStore and Admin gRPC APIs.

## Choose NATS mode

Orisun starts embedded NATS with JetStream enabled by default. Core NATS carries
subscription wake-ups; JetStream provides leases and admin messaging. That is the simplest path for local development, standalone binaries, containers, and embedded Go services.

If you already operate a JetStream-enabled NATS server, set `ORISUN_NATS_URL`:

```bash
ORISUN_NATS_URL=nats://localhost:4222
```

Embedded Go callers can also use Orisun's in-process NATS connection and JetStream handles directly, or pass caller-owned NATS handles. See [Go Embedding](./embedding/go#postgresql-embedding).

SQLite is single-node only. Keep `ORISUN_NATS_CLUSTER_ENABLED=false` for SQLite deployments.

### When to choose SQLite

Choose SQLite when one active Orisun node is enough and operational simplicity matters more than clustering. SQLite mode is complete: it stores the event log, admin state, index metadata, projector checkpoints, and JSON criteria indexes. Event logs live in one `{boundary}.db` file per boundary; derived operational state lives in one `{boundary}_metadata.db` file per boundary.

### When to choose PostgreSQL

Choose PostgreSQL when you need multiple Orisun nodes, want database-level operational tooling, or expect the event log to grow beyond a single-node operational profile.

## Install a binary

Download a release asset for your OS, architecture, and backend from [GitHub Releases](https://github.com/OrisunLabs/Orisun/releases). Asset names follow this pattern:

| Binary | Includes |
| --- | --- |
| `orisun-pg-<os>-<arch>` | PostgreSQL |
| `orisun-sqlite-<os>-<arch>` | SQLite only |

For example, on Linux amd64:

```bash
VERSION="<release-version>"

curl -L \
  "https://github.com/OrisunLabs/Orisun/releases/download/v${VERSION}/orisun-sqlite-linux-amd64" \
  -o orisun-sqlite

chmod +x ./orisun-sqlite
```

Use a binary built from this release with the current client contract. To build
the checked-out source locally:

```bash
./build.sh "$(go env GOOS)" "$(go env GOARCH)" dev sqlite
cp "./build/orisun-sqlite-$(go env GOOS)-$(go env GOARCH)" ./orisun-sqlite

# For PostgreSQL instead:
./build.sh "$(go env GOOS)" "$(go env GOARCH)" dev pg
cp "./build/orisun-pg-$(go env GOOS)-$(go env GOARCH)" ./orisun-pg
```

## Run SQLite from a binary

SQLite is the fastest way to start a local or single-node Orisun server without a separate database.

```bash
mkdir -p ./data/orisun/sqlite ./data/orisun/nats

ORISUN_BACKEND=sqlite \
ORISUN_SQLITE_DIR=./data/orisun/sqlite \
ORISUN_NATS_STORE_DIR=./data/orisun/nats \
ORISUN_NATS_CLUSTER_ENABLED=false \
ORISUN_ADMIN_BOUNDARY=orisun_admin \
ORISUN_ADMIN_USERNAME=admin \
ORISUN_ADMIN_PASSWORD=changeit \
./orisun-sqlite
```

SQLite production deployments should use `ORISUN_SQLITE_SYNCHRONOUS=FULL`
(the recommended setting). `NORMAL` is only for deployments that knowingly trade
crash/power-loss durability for write throughput.

## Run PostgreSQL from a binary

PostgreSQL mode expects an existing PostgreSQL database. Create the database with your normal database tooling; Orisun initializes the
configured admin schema and catalogued boundary storage, then start Orisun with connection settings:

```bash
ORISUN_BACKEND=postgres \
ORISUN_PG_HOST=localhost \
ORISUN_PG_PORT=5432 \
ORISUN_PG_USER=postgres \
ORISUN_PG_PASSWORD='password@1' \
ORISUN_PG_NAME=orisun \
ORISUN_PG_ADMIN_SCHEMA=admin \
ORISUN_ADMIN_BOUNDARY=orisun_admin \
ORISUN_ADMIN_USERNAME=admin \
ORISUN_ADMIN_PASSWORD=changeit \
ORISUN_NATS_STORE_DIR=./data/orisun/nats \
./orisun-pg
```

## Run SQLite with Docker

Create `docker-compose.yml`:

```yaml
services:
  orisun:
    image: orisunlabs/orisun:sqlite
    environment:
      ORISUN_BACKEND: sqlite
      ORISUN_SQLITE_DIR: /var/lib/orisun/sqlite
      ORISUN_NATS_STORE_DIR: /var/lib/orisun/nats
      ORISUN_NATS_CLUSTER_ENABLED: "false"
      ORISUN_ADMIN_BOUNDARY: orisun_admin
      ORISUN_ADMIN_USERNAME: admin
      ORISUN_ADMIN_PASSWORD: changeit
    ports:
      - "5005:5005"
      - "8991:8991"
    volumes:
      - orisun-data:/var/lib/orisun
    restart: unless-stopped

volumes:
  orisun-data:
```

Start the server:

```bash
docker compose up -d
```

## Run PostgreSQL with Docker Compose

Create `docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:17.5-alpine3.22
    environment:
      POSTGRES_DB: orisun
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password@1
    ports:
      - "5434:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  orisun:
    image: orisunlabs/orisun:pg
    environment:
      ORISUN_PG_HOST: postgres
      ORISUN_PG_PORT: 5432
      ORISUN_PG_USER: postgres
      ORISUN_PG_PASSWORD: password@1
      ORISUN_PG_NAME: orisun
      ORISUN_PG_ADMIN_SCHEMA: admin
      ORISUN_NATS_STORE_DIR: /var/lib/orisun/nats
      ORISUN_ADMIN_BOUNDARY: orisun_admin
      ORISUN_ADMIN_USERNAME: admin
      ORISUN_ADMIN_PASSWORD: changeit
    ports:
      - "5005:5005"
      - "8991:8991"
    volumes:
      - orisun-data:/var/lib/orisun
    depends_on:
      - postgres
    restart: unless-stopped

volumes:
  postgres-data:
  orisun-data:
```

Start the stack:

```bash
docker compose up -d
```

## Verify the API

The examples use the default `admin:changeit` credentials.

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
grpcurl -plaintext -H "$AUTH" localhost:5005 list
```

Expected services include:

```text
orisun.Admin
orisun.EventStore
```

:::warning
Change `ORISUN_ADMIN_PASSWORD` before production use.
:::

## Create the application boundary

A fresh installation contains only the admin boundary. Create the application
boundary through the event-backed Admin API before writing application events.
Use the placement for the backend you started:

```bash
# SQLite
grpcurl -plaintext -H "$AUTH" \
  -d '{"name":"orders","description":"Order events","placement":{"backend":"sqlite","namespace":"orders"}}' \
  localhost:5005 orisun.Admin/CreateBoundary

# PostgreSQL
grpcurl -plaintext -H "$AUTH" \
  -d '{"name":"orders","description":"Order events","placement":{"backend":"postgres","namespace":"public"}}' \
  localhost:5005 orisun.Admin/CreateBoundary
```

`CreateBoundary` returns `PROVISIONING`. Wait until `GetBoundary` reports
`BOUNDARY_LIFECYCLE_STATUS_ACTIVE` before sending the first write. If a
clustered node briefly returns `FAILED_PRECONDITION`, retry while its local
runtime installation converges:

```bash
grpcurl -plaintext -H "$AUTH" \
  -d '{"name":"orders"}' \
  localhost:5005 orisun.Admin/GetBoundary
```

PostgreSQL application placements are catalog state; they are not configured
through environment mappings.

## Save your first event

This first write is deliberately unconditional, so it omits `consistency`:

```bash
grpcurl -plaintext -H "$AUTH" -d @ localhost:5005 orisun.EventStore/SaveEventsV2 <<EOF
{
  "boundary": "orders",
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

For a fresh SQLite `orders` boundary, the response contains its write ID and
committed log position (other backends can assign different position values):

```json
{
  "log_position": {
    "commit_position": 1,
    "prepare_position": 1
  },
  "write_id": "1:1"
}
```

Orisun stores the API `event_type` value in event `data` as the canonical `__eventType` JSON key and derives returned event types from that key. You do not need to duplicate it in your payload, and later queries or indexes can match `__eventType` with normal content criteria.

Application commands that read before writing should not remain
unconditional. Preserve each complete query and its latest matching position
in `SaveEventsV2.consistency`; the [Tutorial](./tutorial) builds that full loop.

## Release artifacts

Orisun publishes both release binaries and Docker images.

Binary assets are attached to each GitHub release:

| Asset | Backend |
| --- | --- |
| `orisun-pg-linux-amd64`, `orisun-pg-darwin-arm64`, ... | PostgreSQL |
| `orisun-sqlite-linux-amd64`, `orisun-sqlite-darwin-arm64`, ... | SQLite only |

Docker images are published to Docker Hub and GitHub Container Registry with the same backend flavor tags:

| Tag | Backend |
| --- | --- |
| `orisunlabs/orisun:pg` | PostgreSQL |
| `orisunlabs/orisun:sqlite` | SQLite only |
| `orisunlabs/orisun:<version>-pg` | PostgreSQL release |
| `orisunlabs/orisun:<version>-sqlite` | SQLite-only release |

Use the same tag names under `ghcr.io/orisunlabs/orisun`, for example `ghcr.io/orisunlabs/orisun:<version>-pg`.

## Next steps

- Learn the consistency model in [Command Context Consistency](./concepts/command-context-consistency).
- Read and subscribe with the [EventStore API](./api/eventstore).
- Review production settings in [Configuration](./operations/configuration).
