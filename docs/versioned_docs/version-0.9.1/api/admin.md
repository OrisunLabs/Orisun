---
title: Admin API
description: Manage boundaries, users, credentials, and lightweight operational counts.
---

The Admin service handles the event-backed boundary catalog, users, credentials,
and lightweight operational statistics. It does not manage event indexes; index
management belongs to the EventStore service so embedded deployments can manage
indexes without exposing Admin.

Admin is available in every server flavor:

- PostgreSQL-compatible server
- SQLite-only server
- FoundationDB server

## Authentication

Admin calls use the same Basic auth header as EventStore calls:

```bash
AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
```

The default credentials are `admin:changeit`.

:::warning
Set a production password with `ORISUN_ADMIN_PASSWORD` before exposing the server.
:::

## Methods

| Method | Purpose |
| --- | --- |
| `CreateBoundary` | Define and asynchronously provision new or existing physical storage. |
| `ListBoundaries` | Rebuild and return the boundary catalog. |
| `GetBoundary` | Return one boundary and its current lifecycle state. |
| `CreateUser` | Create an admin or application user. |
| `DeleteUser` | Delete a user by ID. Users cannot delete their own account. |
| `ChangePassword` | Change the authenticated user's password. |
| `ListUsers` | List non-deleted users. |
| `ValidateCredentials` | Validate a username/password pair. |
| `GetUserCount` | Return total active user count. |
| `GetEventCount` | Return event count for a boundary. |

## Boundary lifecycle

Boundary definitions and lifecycle transitions are durable events in the admin
boundary. `CreateBoundary` appends a definition event and returns after that
append commits; physical provisioning continues asynchronously.

| Status | Meaning |
| --- | --- |
| `BOUNDARY_LIFECYCLE_STATUS_PROVISIONING` | The definition is durable, but the backend, runtime registry, publisher, and projectors may not be ready yet. |
| `BOUNDARY_LIFECYCLE_STATUS_ACTIVE` | Shared physical provisioning completed. Each server independently installs the activated boundary before accepting requests for it. |
| `BOUNDARY_LIFECYCLE_STATUS_FAILED` | A provisioning attempt failed and no later activation has been recorded. Inspect `last_error`; the server retries the definition independently with capped exponential backoff. |

Do not send EventStore requests until `GetBoundary` reports `ACTIVE`. A
successful definition RPC is not proof that provisioning succeeded. In a
cluster, a node can briefly return `FAILED_PRECONDITION` after the shared
catalog becomes `ACTIVE` while that node finishes its local runtime install;
retry that request or temporarily remove the lagging node from routing. Failed
definitions remain in the catalog and cannot be re-created under the same name;
the existing definition continues to be retried and may later transition from
`FAILED` to `ACTIVE`.

`BoundaryInfo` also reports:

| Field | Meaning |
| --- | --- |
| `existed_before_catalog` | Whether the physical storage predated its catalog definition. |
| `placement` | The durable backend and immutable physical namespace recorded by the definition event. |
| `last_error` | Recorded provisioning error; empty after activation. |
| `definition_position` | Position of the definition event in the admin boundary. |
| `status_position` | Position of the latest lifecycle event. |

There is currently no rename, placement update, or delete RPC. A boundary name
has one immutable definition.

### Placement rules

| Runtime backend | `placement.backend` | `placement.namespace` |
| --- | --- | --- |
| PostgreSQL or YugabyteDB | `postgres` | PostgreSQL schema name. Multiple boundaries may share a schema because physical objects are boundary-prefixed. |
| SQLite | `sqlite` | Must exactly equal the boundary name. Files are created or opened beneath `ORISUN_SQLITE_DIR`. |
| FoundationDB | `foundationdb` | Must equal the configured `ORISUN_FDB_ROOT`. |

Boundary and PostgreSQL schema identifiers use the portable identifier
contract: 1–63 characters, starting with a letter or underscore, followed by
letters, digits, or underscores.

## CreateBoundary

Use `CreateBoundary` to define a boundary and idempotently ensure its physical
storage:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.Admin/CreateBoundary <<EOF
{
  "name": "orders",
  "description": "Order lifecycle events",
  "placement": {
    "backend": "postgres",
    "namespace": "public"
  }
}
EOF
```

The response initially contains `BOUNDARY_LIFECYCLE_STATUS_PROVISIONING`.

When adopting physical storage that already exists, for example after restoring
a PostgreSQL schema or attaching SQLite boundary files, set
`existed_before_catalog`:

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.Admin/CreateBoundary <<EOF
{
  "name": "legacy_orders",
  "description": "Orders migrated from the legacy deployment",
  "existed_before_catalog": true,
  "placement": {
    "backend": "postgres",
    "namespace": "legacy"
  }
}
EOF
```

The provisioner uses the same idempotent migration and activation flow in both
cases. Legacy boundaries discovered at startup are recorded automatically with
this flag. See
[Boundary management and migration](../operations/configuration#boundary-management).

## ListBoundaries

```bash
grpcurl -H "$AUTH" localhost:5005 orisun.Admin/ListBoundaries
```

The response includes active, provisioning, and failed definitions. Use it for
readiness checks and operational inventory.

## GetBoundary

```bash
grpcurl -H "$AUTH" \
  -d '{"name":"orders"}' \
  localhost:5005 orisun.Admin/GetBoundary
```

An unknown name returns `NOT_FOUND`.

### Definition errors

| gRPC code | Typical cause |
| --- | --- |
| `INVALID_ARGUMENT` | Missing placement, invalid name, or empty backend/namespace. |
| `ALREADY_EXISTS` | A definition already exists for that name, including definitions currently `FAILED`. |
| `NOT_FOUND` | `GetBoundary` cannot find the requested definition. |

Backend and namespace compatibility is checked by the asynchronous provisioner.
An incompatible non-empty placement can therefore make the definition RPC
succeed and the boundary subsequently become `FAILED`.

:::warning
All Admin RPCs, including boundary creation, currently require
authentication but are not role-gated. Restrict access to the Admin service at
the network and credential layers. See
[Security & Authorization](../operations/security).
:::

## CreateUser

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.Admin/CreateUser <<EOF
{
  "name": "Ops User",
  "username": "ops",
  "password": "change-this",
  "roles": ["OPERATIONS"]
}
EOF
```

Required fields:

| Field | Description |
| --- | --- |
| `name` | Human-readable name. |
| `username` | Unique login username. |
| `password` | Initial password. |
| `roles` | Role list. Valid values are `ADMIN` and `OPERATIONS`. |

:::warning
Roles are matched exactly and are case-sensitive. Use the uppercase values `ADMIN` and `OPERATIONS`; a value like `admin` is stored verbatim and never satisfies a role check, so the user is authenticated but authorized for nothing role-gated. See [Security & Authorization](../operations/security).
:::

## ListUsers

```bash
grpcurl -H "$AUTH" localhost:5005 orisun.Admin/ListUsers
```

## DeleteUser

```bash
grpcurl -H "$AUTH" \
  -d '{"user_id":"550e8400-e29b-41d4-a716-446655440000"}' \
  localhost:5005 orisun.Admin/DeleteUser
```

Authenticated users cannot delete their own account.

## ChangePassword

```bash
grpcurl -H "$AUTH" -d @ localhost:5005 orisun.Admin/ChangePassword <<EOF
{
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "current_password": "changeit",
  "new_password": "replace-with-a-strong-password"
}
EOF
```

Users can only change their own password.

## ValidateCredentials

```bash
grpcurl -H "$AUTH" \
  -d '{"username":"ops","password":"change-this"}' \
  localhost:5005 orisun.Admin/ValidateCredentials
```

The response includes `success` and, when validation succeeds, the matching user.

## GetUserCount

```bash
grpcurl -H "$AUTH" localhost:5005 orisun.Admin/GetUserCount
```

## GetEventCount

```bash
grpcurl -H "$AUTH" \
  -d '{"boundary":"orders"}' \
  localhost:5005 orisun.Admin/GetEventCount
```

## Proto source

The Admin protobuf source lives at [`proto/admin.proto`](https://github.com/OrisunLabs/Orisun/blob/main/proto/admin.proto).
