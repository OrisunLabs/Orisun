---
title: Go Embedding
description: Run Orisun directly inside a Go service.
---

Go services can embed Orisun directly instead of running the gRPC server as a separate process.

This guide targets Orisun `0.8.0`. Install the canonical module path with:

```bash
go get github.com/OrisunLabs/Orisun@v0.8.0
```

If you are upgrading from `v0.7.0`, follow the
[0.7.0 to 0.8.0 upgrade guide](../operations/upgrading-0.7-to-0.8#embedded-go-changes)
for the generated-type import move, subscription callback change, and boundary
catalog migration.

Backend-specific embedding packages keep deployments explicit:

- `embedded/postgres` imports the PostgreSQL backend.
- `embedded/sqlite` imports the SQLite backend.
- `embedded/foundationdb` imports the FoundationDB backend when built with `-tags foundationdb`.
- Neither package needs the unused backend.

Use embedding when Orisun should be part of your service process. Use the standalone server when you want a separate operational boundary and language-agnostic gRPC access.

## PostgreSQL Embedding

```go
import (
	"context"

	embeddedpg "github.com/OrisunLabs/Orisun/embedded/postgres"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
)

func start(ctx context.Context) (*embeddedpg.Store, error) {
	cfg := config.InitializeConfig()
	cfg.Backend.Type = "postgres"
	logger := logging.InitializeDefaultLogger(cfg.Logging)
	return embeddedpg.Start(ctx, cfg, logger)
}
```

By default, embedded stores start embedded NATS JetStream in the same process.
Use the store's NATS handles when your host process wants direct access without connecting to a NATS URL:

```go
nc := store.NATSConnection()
js := store.JetStream()
```

To use an existing JetStream-enabled NATS server instead:

```go
store, err := embeddedpg.Start(
	ctx,
	cfg,
	logger,
	embeddedpg.WithNATSURL("nats://localhost:4222"),
)
```

If your service already owns a NATS connection or JetStream handle, pass it directly:

```go
store, err := embeddedpg.Start(ctx, cfg, logger, embeddedpg.WithNATSConnection(conn))
store, err = embeddedpg.Start(ctx, cfg, logger, embeddedpg.WithJetStream(js))
```

## SQLite Embedding

```go
import (
	"context"

	embeddedsqlite "github.com/OrisunLabs/Orisun/embedded/sqlite"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
)

func start(ctx context.Context) (*embeddedsqlite.Store, error) {
	cfg := config.InitializeConfig()
	cfg.Backend.Type = "sqlite"
	cfg.Nats.Cluster.Enabled = false
	logger := logging.InitializeDefaultLogger(cfg.Logging)
	return embeddedsqlite.Start(ctx, cfg, logger)
}
```

SQLite embedding supports the same NATS options:

```go
store, err := embeddedsqlite.Start(
	ctx,
	cfg,
	logger,
	embeddedsqlite.WithNATSURL("nats://localhost:4222"),
)
```

SQLite remains single-node only. Keep `cfg.Nats.Cluster.Enabled = false`.

## FoundationDB Embedding

FoundationDB embedding is beta. It requires the native FoundationDB client libraries and a build with the `foundationdb` tag. Treat FDB storage layout and operational defaults as subject to breaking change until the backend graduates from beta.

```go
import (
	"context"

	embeddedfdb "github.com/OrisunLabs/Orisun/embedded/foundationdb"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
)

func start(ctx context.Context) (*embeddedfdb.Store, error) {
	cfg := config.InitializeConfig()
	cfg.Backend.Type = "foundationdb"
	cfg.FoundationDB.ClusterFile = "/etc/foundationdb/fdb.cluster"
	logger := logging.InitializeDefaultLogger(cfg.Logging)
	return embeddedfdb.Start(ctx, cfg, logger)
}
```

Use the same NATS options as the other embedded stores. Build the host service with:

```bash
go build -tags foundationdb ./...
```

Application boundaries must exist and be `ACTIVE` before they are used by
`SaveEvents`, reads, subscriptions, or index management. Embedded stores expose
the same event-backed lifecycle as the Admin gRPC API.

## Embedded boundary management

The examples below use:

```go
import (
	"context"
	"fmt"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
)
```

Use `CreateBoundary` when the embedded runtime should create the physical
storage:

```go
created, err := store.CreateBoundary(ctx, boundarymodel.Definition{
	Name:        "orders",
	Description: "Order lifecycle events",
	Placement: boundarymodel.Placement{
		Backend:   "sqlite",
		Namespace: "orders",
	},
})
if err != nil {
	return err
}
_ = created // the initial status is PROVISIONING
```

For PostgreSQL use backend `postgres` and a schema namespace. For FoundationDB
use backend `foundationdb` and the configured root. SQLite requires the
namespace to equal the boundary name.

Creation is asynchronous because the method first emits a durable definition
event. Poll `GetBoundary` before using the boundary:

```go
for {
	boundary, err := store.GetBoundary(ctx, "orders")
	if err != nil {
		return err
	}
	switch boundary.Status {
	case boundarymodel.StatusActive:
		// The runtime registry, publisher, and physical storage are ready.
		goto ready
	case boundarymodel.StatusFailed:
		// Provisioning is retried automatically; expose the current cause.
		return fmt.Errorf("provision orders: %s", boundary.LastError)
	}
	if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
		return err
	}
}

ready:
```

Here `sleepContext` is any context-aware delay used by the host application.
Avoid an unbounded `time.Sleep` loop during shutdown.

```go
func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

Use `CreateBoundary` with `ExistedBeforeCatalog` for physical storage that
already exists:

```go
existing, err := store.CreateBoundary(ctx, boundarymodel.Definition{
	Name:                 "legacy_orders",
	Description:          "Existing order event log",
	ExistedBeforeCatalog: true,
	Placement: boundarymodel.Placement{
		Backend:   "postgres",
		Namespace: "legacy",
	},
})
if err != nil {
	return err
}
_ = existing // wait for ACTIVE
```

The definition is returned as `PROVISIONING`; the provisioner opens the
existing storage, applies migrations idempotently, and then activates it.
Duplicate create commands return an already-exists error because every
successful command must produce exactly one definition event.

Inspect the complete event-rebuilt catalog with:

```go
boundaries, err := store.ListBoundaries(ctx)
if err != nil {
	return err
}
boundary, err := store.GetBoundary(ctx, "orders")
if err != nil {
	return err
}
use(boundaries, boundary)
```

At startup, embedded stores replay catalog definitions into the local runtime.
PostgreSQL uses `ORISUN_PG_ADMIN_SCHEMA` only to locate the admin boundary;
application schema placements come from the catalog. SQLite and FoundationDB
also install application boundaries only from catalog definitions.

An existing PostgreSQL installation from before 0.8.0 must run 0.8.0 first
with its complete `ORISUN_PG_SCHEMAS` mapping. That bridge release imports the
legacy physical boundaries before a later embedded version stops accepting the
mapping list.

## Reading events in-process

Embedded reads skip protobuf materialization. In Orisun `0.6.1`, `GetEvents` returns a packed `ReadEventBatch` whose events carry scalar `CommitPosition` and `PreparePosition` fields and a `time.Time` `DateCreated`:

```go
batch, err := store.GetEvents(ctx, &orisun.GetEventsRequest{
	Boundary: "orders",
	Count:    100,
})
if err != nil {
	return err
}
for _, e := range batch {
	process(e.EventType, e.Data, e.CommitPosition, e.PreparePosition)
}
```

One page is capped at 10,000 events; page forward from the last event's position for larger reads.

For carried-state command contexts, `GetLatestByCriteria` takes a `LatestByCriteriaQuery` and returns a `LatestByCriteriaBatch`. Matches align positionally with the input criteria and expose a `Found` flag:

```go
latest, err := store.GetLatestByCriteria(ctx, orisun.LatestByCriteriaQuery{
	Boundary: "orders",
	Criteria: []orisun.ReadCriterion{
		{Tags: []orisun.ReadTag{
			{Key: "eventType", Value: "OrderPlaced"},
			{Key: "orderId", Value: "o-1"},
		}},
	},
})
if err != nil {
	return err
}
if latest.Matches[0].Found {
	decideFrom(latest.Matches[0].Event)
}
```

Use `latest.ContextCommitPosition` and `latest.ContextPreparePosition` as the expected position for the next `SaveEvents` with the same combined criteria, exactly as over gRPC.

The public gRPC and protobuf contract is unchanged; these packed types apply only to in-process callers.

## Embedded Subscriptions

Embedded subscriptions use the transport-neutral `eventstore` model and an
ordered callback. They do not expose a protobuf event stream:

```go
import (
	"context"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
)

after := coreeventstore.BeginningPosition()
err := store.SubscribeToEvents(
	ctx,
	coreeventstore.SubscribeRequest{
		Boundary:       "orders",
		SubscriberName: "orders-projection",
		AfterPosition:  &after,
	},
	func(ctx context.Context, event coreeventstore.ReadEvent) error {
		return project(ctx, event)
	},
)
```

The callback runs synchronously and receives events in subscription order.
Returning `nil` advances delivery to the next event. Returning an error stops
the subscription and propagates the error to `SubscribeToEvents`; canceling the
context also stops the subscription. Keep the callback bounded, or deliberately
use it to apply backpressure while updating a projection and its checkpoint.

## Embedded Index Management

Embedded stores expose boundary index management directly. Applications do not need to expose Admin to create JSON expression indexes.

```go
err := store.CreateBoundaryIndex(
	ctx,
	"orders",
	"customer_id",
	[]orisun.BoundaryIndexField{
		{JsonKey: "customer_id", ValueType: "text"},
	},
	nil,
	orisun.IndexCombinatorAND,
)
```

## Capabilities

Embedded stores expose the same high-level behavior as the server:

- save events
- query events
- subscribe to events
- create and import boundaries
- list and inspect boundary lifecycle state
- create and drop boundary indexes
- preserve backend-specific publishing guarantees

## Shutdown

Call the store's close method during service shutdown so database pools, NATS resources, and background loops can stop cleanly. Orisun closes NATS resources it creates, but it does not close caller-owned connections or JetStream handles passed with `WithNATSConnection` or `WithJetStream`.
