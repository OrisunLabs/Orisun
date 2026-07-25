---
title: Security & Authorization
description: Authentication, token reuse, role-based authorization, and the per-RPC permission matrix.
---

Orisun authenticates every gRPC call and authorizes a subset of them by role. This page describes both layers and the exact permission each RPC requires.

## Authentication

Every EventStore and Admin call must carry credentials. Two forms are accepted:

- **HTTP Basic**, as the `authorization` metadata header:

  ```bash
  AUTH='Authorization: Basic YWRtaW46Y2hhbmdlaXQ='
  grpcurl -H "$AUTH" localhost:5005 orisun.EventStore/Ping
  ```

- **A session token** in the `x-auth-token` header. Every authenticated response sets `x-auth-token`; a client can send that token on later calls instead of re-sending Basic credentials. The token is validated first, then Basic is used as the fallback.

A missing or malformed header returns `UNAUTHENTICATED`. Invalid credentials also return `UNAUTHENTICATED`.

Session tokens expire after `ORISUN_AUTH_SESSION_TTL` of inactivity, which
defaults to `24h`. Each successful use renews that deadline. A password change,
user deletion, or role change revokes every session for the affected user; the
client must authenticate with Basic credentials again. Tokens are held by the
server process that issued them, so clustered deployments should either route a
client consistently to one node or keep Basic credentials available for
fallback authentication.

The default account is `admin:changeit`.

:::warning
Change `ORISUN_ADMIN_PASSWORD` before exposing the server, and enable [TLS](./configuration#tls-settings) for any non-local deployment. Basic credentials are only as safe as the transport.
:::

## Roles

There are exactly two roles, and they are **case-sensitive**:

| Role | Value |
| --- | --- |
| Administrator | `ADMIN` |
| Operations | `OPERATIONS` |

Role values are validated and compared exactly. A user-creation request with
`admin` (lowercase) or another unsupported value returns `INVALID_ARGUMENT`.

## Permission matrix

| RPC | Authentication | Role required |
| --- | --- | --- |
| `EventStore/SaveEvents` | Yes | `ADMIN` or `OPERATIONS` |
| `EventStore/CreateIndex` | Yes | `ADMIN` |
| `EventStore/DropIndex` | Yes | `ADMIN` |
| `EventStore/GetEvents` | Yes | Any authenticated user |
| `EventStore/GetLatestByCriteria` | Yes | Any authenticated user |
| `EventStore/CatchUpSubscribeToEvents` | Yes | Any authenticated user |
| `EventStore/Ping` | Yes | Any authenticated user |
| `Admin/CreateBoundary` | Yes | `ADMIN` |
| `Admin/ListBoundaries` | Yes | `ADMIN` or `OPERATIONS` |
| `Admin/GetBoundary` | Yes | `ADMIN` or `OPERATIONS` |
| `Admin/CreateUser` | Yes | `ADMIN` |
| `Admin/DeleteUser` | Yes | `ADMIN` |
| `Admin/ChangePassword` | Yes | Any authenticated user, for their own account |
| `Admin/ListUsers` | Yes | `ADMIN` |
| `Admin/ValidateCredentials` | Yes | `ADMIN` |
| `Admin/GetUserCount` | Yes | `ADMIN` |
| `Admin/GetEventCount` | Yes | `ADMIN` or `OPERATIONS` |

Two points are worth calling out:

- **Event reads and subscriptions are not boundary-scoped.** Any authenticated
  user can currently read or subscribe to any active boundary. Use separate
  credentials and network controls when applications must not share event
  data.
- **Administrative mutations require `ADMIN`.** `OPERATIONS` can inspect
  boundary state and event counts, but cannot provision storage or manage
  users. `ChangePassword` remains self-service and only changes the caller's
  own account.

## Recommended posture

- Set a strong `ORISUN_ADMIN_PASSWORD` and create per-application users with the narrowest role they need: `OPERATIONS` for services that only save and read events, `ADMIN` only where index management is required.
- Set `ORISUN_AUTH_SESSION_TTL` to the shortest inactivity window your clients
  can tolerate.
- Enable gRPC TLS and, where mutual auth is needed, `ORISUN_GRPC_TLS_CLIENT_AUTH_REQUIRED`.
- Put PostgreSQL, the gRPC port, and NATS cluster routes behind network policy. See the [Deployment security checklist](./deployment#security-checklist).
- Monitor the event-backed boundary catalog. Treat unexpected definitions or
  placement changes as privileged configuration changes.
