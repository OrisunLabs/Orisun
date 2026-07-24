---
title: Upgrade from 0.7.0 to 0.8.0
description: A production runbook for migrating existing boundaries into the 0.8.0 event-backed catalog.
---

Orisun 0.8.0 replaces the static `ORISUN_BOUNDARIES` startup list with a
durable, event-backed boundary catalog. This is an operator-visible change and,
for embedded Go users, an API-breaking change. Existing application events are
not copied or rewritten: for PostgreSQL-compatible and SQLite deployments, the
first 0.8.0 startup discovers each physical boundary, records a
`BoundaryCreated` definition marked `existed_before_catalog` in the admin
boundary, and activates that boundary in the running server. FoundationDB is
beta and is excluded from this automatic migration.

Use this guide for a direct upgrade from 0.7.0. Test the complete procedure
against a recent production backup before upgrading production.

:::danger Mandatory bridge release
Never skip 0.8.0 when upgrading an existing installation from 0.7.x or
earlier. Version 0.8.0 is the release that converts legacy PostgreSQL schema
mappings and SQLite boundary discovery into the durable catalog used by later
versions. Later PostgreSQL releases refuse to start against a pre-existing
admin event store that has no 0.8.0 catalog marker.
:::

## What changes in 0.8.0

| Area | 0.7.0 | 0.8.0 |
| --- | --- | --- |
| Existing boundaries | Listed in `ORISUN_BOUNDARIES` at every startup. | Recorded once in the admin boundary catalog and replayed from there. |
| New boundaries | Required a configuration change and restart. | Created at runtime with `Admin/CreateBoundary`. |
| Existing storage attached later | Added to startup configuration. | Registered with `Admin/CreateBoundary` and `existed_before_catalog`. |
| Readiness | A configured name was installed during startup. | A definition moves asynchronously from `PROVISIONING` to `ACTIVE` or `FAILED`. |
| Unknown boundaries | Depended on the static startup registry. | Rejected until an active catalog definition is installed locally. |
| PostgreSQL mappings | `ORISUN_PG_SCHEMAS` defined every runtime boundary. | It always bootstraps the admin mapping and is a one-time reconciliation source for legacy mappings. |
| gRPC EventStore API | Existing methods and messages. | Wire-compatible; the boundary Admin RPCs are additive. |
| Embedded Go subscriptions | Generated request/event types and `MessageHandler`. | Transport-neutral `eventstore.SubscribeRequest` and `eventstore.EventHandler`. |

Boundary definitions are immutable. A name has exactly one backend and physical
namespace, and there is no rename, placement-update, or delete RPC in 0.8.0.

## Before the upgrade

1. Record the exact 0.7.0 image, binary, environment, secrets, and mounted
   storage paths used by every node.
2. Inventory the names in `ORISUN_BOUNDARIES` and match every name to its
   physical storage:

   | Backend | Physical source 0.8.0 discovers |
   | --- | --- |
   | PostgreSQL or YugabyteDB | Every existing `boundary:schema` entry in `ORISUN_PG_SCHEMAS`. |
   | SQLite | Existing `{boundary}.db` files in `ORISUN_SQLITE_DIR`. |

3. Resolve missing, duplicate, or incorrect mappings before upgrading. The
   admin boundary must be present, and each PostgreSQL application boundary
   must retain its exact 0.7.0 schema mapping.
4. Take a coordinated backup of the durable store, including the complete
   admin boundary. For SQLite, stop the single writer or use the SQLite backup
   API and include every event and metadata database. For PostgreSQL or
   YugabyteDB, back up the database containing both application and admin
   objects.
5. Restore that backup in staging and complete this runbook there first.
6. Upgrade application code that embeds Orisun and compile it before the
   production rollout. See [Embedded Go changes](#embedded-go-changes).

:::danger
Do not delete legacy mappings or move SQLite files before the first successful
0.8.0 startup. Those are the discovery sources used to build the durable
catalog.
:::

## Prepare the 0.8.0 configuration

### PostgreSQL and YugabyteDB

Keep every existing mapping for the first 0.8.0 startup. For example:

```bash
ORISUN_ADMIN_BOUNDARY=orisun_admin
ORISUN_PG_SCHEMAS=orders:public,payments:public,orisun_admin:admin
```

The 0.8.0 default contains only `orisun_admin:admin`. Do not rely on that
default during migration: set the complete legacy mapping explicitly on the
first upgraded node.

The mapping for `ORISUN_ADMIN_BOUNDARY` is always required. After all nodes run
0.8.0 and the catalog is verified, non-admin mappings may be removed:

```bash
ORISUN_PG_SCHEMAS=orisun_admin:admin
```

When subsequently upgrading beyond 0.8.0, replace that remaining mapping with:

```bash
ORISUN_PG_ADMIN_SCHEMA=admin
```

Do not reduce this setting while any 0.7.0 node remains. Those nodes still use
the complete mapping list at startup.

### SQLite

Keep `ORISUN_SQLITE_DIR` unchanged and keep all boundary files in that
directory. SQLite supports one active Orisun node only, so use a stop, upgrade,
start rollout rather than a mixed-version rollout.

### FoundationDB

FoundationDB is beta and is intentionally excluded from automatic legacy
catalog migration. Existing development data is not discovered from tuple key
ranges. Define beta-backend boundaries explicitly through `CreateBoundary`, or
start with a fresh root when testing 0.8.0.

### Remove `ORISUN_BOUNDARIES`

0.8.0 does not use `ORISUN_BOUNDARIES`. Remove it from the 0.8.0 manifest,
service definition, Helm values, and secret/config templates.

If 0.7.0 and 0.8.0 nodes temporarily share one configuration template, keep
`ORISUN_BOUNDARIES` until the last 0.7.0 node is drained, then remove it. It is
ignored by 0.8.0 and still required by 0.7.0.

## Roll out the server

For a PostgreSQL or YugabyteDB cluster:

1. Prevent the first 0.8.0 node from receiving application traffic.
2. Start that node with the unchanged storage location and the legacy discovery
   settings described above.
3. Wait for gRPC readiness. Startup reconciles every discovered physical
   boundary through the durable `CreateBoundary` path with
   `existed_before_catalog`.
4. Verify the catalog and resolve every failure before routing traffic to the
   node.
5. Upgrade the remaining nodes one at a time. Each node replays the shared
   catalog, installs every active boundary into its local runtime, and joins
   publisher coordination.
6. Drain the final 0.7.0 node.
7. Remove `ORISUN_BOUNDARIES`. PostgreSQL-compatible deployments may also
   reduce `ORISUN_PG_SCHEMAS` to the required admin mapping.

For SQLite, stop the 0.7.0 process, take the final backup, replace the binary or
image, and start one 0.8.0 process against the unchanged directory.

:::warning
Do not create new runtime boundaries during a mixed 0.7.0/0.8.0 rollout.
0.7.0 nodes do not replay the new catalog and cannot serve those boundaries.
Wait until every traffic-serving node runs 0.8.0.
:::

Reconciliation is idempotent. Restarting 0.8.0 with the same discovered
placement does not create a second definition. If discovery finds a placement
that conflicts with the immutable catalog entry, startup fails instead of
silently pointing a boundary at different storage.

## Verify the migration

Set the Admin authentication header and inspect the rebuilt catalog:

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
grpcurl -H "$AUTH" localhost:5005 orisun.Admin/ListBoundaries
```

Use your configured credentials rather than the default header shown above.
The response must contain:

- the configured admin boundary;
- every application boundary from the pre-upgrade inventory;
- the expected backend and namespace for each boundary;
- `existed_before_catalog: true` for migrated definitions; and
- `BOUNDARY_LIFECYCLE_STATUS_ACTIVE` with an empty `last_error` for every
  definition.

An optional machine-readable gate with `jq` is:

```bash
grpcurl -H "$AUTH" localhost:5005 orisun.Admin/ListBoundaries |
  jq -e '
    (.boundaries | length) > 0 and
    all(.boundaries[];
      .status == "BOUNDARY_LIFECYCLE_STATUS_ACTIVE" and
      ((.lastError // "") == "")
    )
  '
```

Check a specific boundary when the inventory or placement needs closer
inspection:

```bash
grpcurl -H "$AUTH" \
  -d '{"name":"orders"}' \
  localhost:5005 orisun.Admin/GetBoundary
```

Before completing the rollout:

1. Read a known event page from every important boundary.
2. Perform a controlled write using the normal expected-position/CCC path.
3. Confirm catch-up and live subscriptions advance.
4. Confirm index management still works for each boundary.
5. Check logs and metrics for reconciliation, provisioning, publisher-lock, and
   projector errors.
6. Restart the canary once and verify the catalog is replayed without duplicate
   definitions or placement conflicts.

## If a boundary is not active

`PROVISIONING` means the definition is durable but the local backend, runtime,
publisher, or projectors may not be ready. Wait and query it again.

`FAILED` means the latest provisioning attempt failed. Inspect `last_error` and
server logs, then correct the underlying connectivity, permissions, storage, or
placement issue. The server retries failed definitions independently with
backoff; do not call `CreateBoundary` again under the same name.

Common migration failures:

| Symptom | Action |
| --- | --- |
| PostgreSQL boundary is absent | Restore its exact `boundary:schema` entry to `ORISUN_PG_SCHEMAS` and restart the canary. |
| SQLite boundary is absent | Restore the files to the unchanged `ORISUN_SQLITE_DIR`. Storage attached after startup must be registered explicitly with `CreateBoundary` and `existed_before_catalog`. |
| Placement conflict stops startup | Make the discovery setting match the already-recorded placement. Do not rename or remap the physical boundary in place. |
| EventStore returns `FAILED_PRECONDITION` | The boundary is unknown or not active on that node. Check `GetBoundary` and local provisioning logs. |

See [Troubleshooting](./troubleshooting#boundary-not-found) for the complete
diagnostic flow.

## Client rollout

The EventStore gRPC wire contract remains compatible. Existing 0.7.0 clients
can continue to read, write, and subscribe to boundaries that are active in the
0.8.0 server. Deploy servers and verify migrated boundaries before updating
applications.

Update to the Go, Node.js, or Java client release that accompanies Orisun 0.8.0
when an application needs to create, list, or inspect boundaries.
`CreateBoundary` returns while the definition is still
`PROVISIONING`; poll `GetBoundary` until it reports `ACTIVE` before issuing
EventStore operations. A clustered node may briefly return
`FAILED_PRECONDITION` while installing the active definition locally; retry or
remove that node from routing until it converges. See
[Client libraries](../api/clients#boundary-prerequisite).

## Embedded Go changes

Embedded applications must be rebuilt against 0.8.0.

Generated protobuf request, response, event, and gRPC client types moved from:

```go
github.com/OrisunLabs/Orisun/orisun
```

to:

```go
github.com/OrisunLabs/Orisun/orisun/grpcapi
```

The server module's transport-neutral subscription API now accepts an
`eventstore.SubscribeRequest` and an `eventstore.EventHandler`:

```go
import (
	"context"

	"github.com/OrisunLabs/Orisun/eventstore"
)

err := store.SubscribeToEvents(
	ctx,
	eventstore.SubscribeRequest{
		Boundary:       "orders",
		SubscriberName: "order-projector",
	},
	func(ctx context.Context, event eventstore.ReadEvent) error {
		// Persist the projection and its checkpoint before returning.
		return nil
	},
)
```

Replace `MessageHandler[orisun.Event]` subscription plumbing with this callback.
Returning an error stops the subscription; cancellation comes from the callback
context. Domain types used by embedded code remain outside `orisun/grpcapi`.

Embedded PostgreSQL, SQLite, and FoundationDB stores also expose
`CreateBoundary`, `ListBoundaries`, and `GetBoundary` with
transport-neutral types. See [Go Embedding](../embedding/go#embedded-boundary-management).

## Rollback

Treat downgrade after a production 0.8.0 startup as a restore operation, not an
in-place binary swap:

1. Stop traffic and all Orisun nodes.
2. Preserve the failed 0.8.0 state for diagnosis.
3. Restore the coordinated pre-upgrade durable-store backup, including the
   admin boundary.
4. Restore the 0.7.0 binary/image and its complete
   `ORISUN_BOUNDARIES`/backend configuration.
5. Verify reads, writes, and subscriptions before reopening traffic.

Restoring a pre-upgrade backup discards writes made after that backup. Choose a
maintenance or traffic-drain strategy that makes this recovery point acceptable.
Do not delete catalog events or physical storage manually to simulate a
downgrade.

## After the upgrade

For every new boundary:

1. Call `CreateBoundary` with the target backend and namespace.
2. Poll `GetBoundary` until the definition is `ACTIVE`.
3. Create the required indexes.
4. Begin application reads, writes, and subscriptions.

Set `existed_before_catalog` on `CreateBoundary` when physical storage already
exists, such as a restored PostgreSQL schema or copied SQLite boundary files.
Keep the admin boundary in backup, restore, monitoring, and disaster-recovery
procedures: it now contains the authoritative boundary catalog.
