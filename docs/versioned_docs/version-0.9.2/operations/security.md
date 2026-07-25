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

A role string is stored exactly as supplied and compared exactly. A user created with `admin` (lowercase) authenticates but matches no role check, so every role-gated call returns `PERMISSION_DENIED`. Always use the uppercase values.

## Permission matrix

| RPC | Authentication | Role required |
| --- | --- | --- |
| `EventStore/SaveEvents` | Yes | `ADMIN` or `OPERATIONS` |
| `EventStore/CreateIndex` | Yes | `ADMIN` |
| `EventStore/DropIndex` | Yes | `ADMIN` |
| `EventStore/GetEvents` | Yes | Any authenticated user |
| `EventStore/CatchUpSubscribeToEvents` | Yes | Any authenticated user |
| `EventStore/Ping` | Yes | Any authenticated user |
| `Admin/*` | Yes | Any authenticated user |

Two points worth calling out:

- **Reads and subscriptions are not role-gated.** Any authenticated user can read or subscribe to any boundary. Use separate credentials and network controls if you need to restrict who can read event data.
- **Admin RPCs are not role-gated in the handler.** Authentication is required, but user and boundary management are gated by authentication alone plus per-operation self-rules (a user cannot delete their own account, and `ChangePassword` only changes the caller's own password). Any authenticated account can currently call `CreateBoundary`, which can create or attach physical storage and start runtime resources. Restrict network access and credentials for the Admin surface accordingly.

## Recommended posture

- Set a strong `ORISUN_ADMIN_PASSWORD` and create per-application users with the narrowest role they need: `OPERATIONS` for services that only save and read events, `ADMIN` only where index management is required.
- Enable gRPC TLS and, where mutual auth is needed, `ORISUN_GRPC_TLS_CLIENT_AUTH_REQUIRED`.
- Put PostgreSQL, the gRPC port, and NATS cluster routes behind network policy. See the [Deployment security checklist](./deployment#security-checklist).
- Monitor the event-backed boundary catalog. Treat unexpected definitions or
  placement changes as privileged configuration changes even though the
  current RPC handler does not enforce an `ADMIN` role.
