---
title: Clients
description: Official client libraries and protobuf sources.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Orisun exposes gRPC services. You can use the official typed clients, `grpcurl`, or generate bindings for another language from the protobuf files.

## Official Clients

| Language | Package | Repository |
| --- | --- | --- |
| Go | `github.com/oexza/orisun-client-go` | [`orisun-client-go`](https://github.com/OrisunLabs/orisun-client-go) |
| Node.js | `@orisun/eventstore-client` | [`orisun-node-client`](https://github.com/OrisunLabs/orisun-node-client) |
| Java | `com.orisunlabs:orisun-java-client` | [`orisun-client-java`](https://github.com/OrisunLabs/orisun-client-java) |

## Install

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```bash
go get github.com/oexza/orisun-client-go
```

  </TabItem>
  <TabItem value="node" label="Node.js">

The package is currently installed from GitHub (npm publication is pending):

```bash
npm install github:OrisunLabs/orisun-node-client
```

Or in `package.json`:

```json
{
  "dependencies": {
    "@orisun/eventstore-client": "github:OrisunLabs/orisun-node-client"
  }
}
```

  </TabItem>
  <TabItem value="java" label="Java">

Published to GitHub Packages. Add the repository and dependency, then authenticate with a GitHub personal access token.

**Maven** (`pom.xml`):

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/OrisunLabs/orisun-client-java</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.orisunlabs</groupId>
        <artifactId>orisun-java-client</artifactId>
        <version>0.0.1</version>
    </dependency>
</dependencies>
```

**Gradle** (`build.gradle`):

```groovy
repositories {
    maven {
        url = 'https://maven.pkg.github.com/OrisunLabs/orisun-client-java'
        credentials {
            username = System.getenv('GITHUB_USERNAME')
            password = System.getenv('GITHUB_TOKEN')
        }
    }
}

dependencies {
    implementation 'com.orisunlabs:orisun-java-client:0.0.1'
}
```

  </TabItem>
</Tabs>

Use the typed clients when you want request/response objects and subscription helpers. Use `grpcurl` for operational checks, debugging, and quick experiments.

## Boundary prerequisite

Client event operations target an active boundary; a boundary name is not
created implicitly by the first write. Before starting an application, define
the storage with `Admin/CreateBoundary`—setting `existed_before_catalog` when
adopting existing physical storage—then wait for `Admin/GetBoundary` to report
`BOUNDARY_LIFECYCLE_STATUS_ACTIVE`. See the
[Admin boundary API](./admin#boundary-lifecycle) for placements, lifecycle
states, and errors.

The Node client exposes typed helpers:

```typescript
import { AdminClient, BoundaryStatus } from '@orisun/eventstore-client';

const admin = new AdminClient({
  host: 'localhost',
  port: 5005,
  username: 'admin',
  password: 'changeit',
});

const {boundary: definition} = await admin.createBoundary({
  name: 'accounts',
  description: 'Account lifecycle events',
  placement: {backend: 'postgres', namespace: 'public'},
});

let boundary = definition;
while (boundary.status === BoundaryStatus.PROVISIONING) {
  await new Promise(resolve => setTimeout(resolve, 100));
  ({boundary} = await admin.getBoundary('accounts'));
}
if (boundary.status !== BoundaryStatus.ACTIVE) {
  throw new Error(`Boundary provisioning failed: ${boundary.lastError}`);
}

const {boundaries} = await admin.listBoundaries();
```

Set `existedBeforeCatalog: true` on `createBoundary(...)` when the physical
storage already exists. The command returns a definition in `PROVISIONING`; do
not treat the response as readiness. In clustered deployments, retry an EventStore
request that briefly returns `FAILED_PRECONDITION` after the shared catalog
becomes `ACTIVE`; the selected node is still completing its local runtime
installation.

## First program

A complete command loop: connect, save an event with a Command Context Consistency check, read the latest carried state from one snapshot, handle a conflict, and subscribe a projector. The examples assume the `accounts` boundary has already reached `ACTIVE`. Pick your language once; the same `groupId` carries over to the [EventStore API](./eventstore) and [Tutorial](../tutorial) pages.

<Tabs groupId="client-lang">
  <TabItem value="go" label="Go" default>

```go
package main

import (
	"context"
	"errors"
	"log"

	orisun "github.com/oexza/orisun-client-go"
	eventstore "github.com/oexza/orisun-client-go/eventstore"
)

func main() {
	client, err := orisun.New(
		"localhost:5005",
		orisun.WithCredentials("admin", "changeit"),
		orisun.WithInsecure(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	accountOpenedID := "018f2d5e-0001-7000-8000-000000000001"
	accountRootQuery := &eventstore.Query{
		Criteria: []*eventstore.Criterion{{
			Tags: []*eventstore.Tag{
				{Key: "eventType", Value: "AccountOpened"},
				{Key: "accountOpenedId", Value: accountOpenedID},
			},
		}},
	}
	accountContextQuery := &eventstore.Query{
		Criteria: []*eventstore.Criterion{
			accountRootQuery.Criteria[0],
			{Tags: []*eventstore.Tag{{Key: "scopes.accountOpenedId", Value: accountOpenedID}}},
		},
	}

	// 1. Open the account (context must be empty).
	_, err = client.SaveEvents(ctx, &eventstore.SaveEventsRequest{
		Boundary: "accounts",
		Query: &eventstore.SaveQuery{
			ExpectedPosition: &eventstore.Position{CommitPosition: -1, PreparePosition: -1},
			SubsetQuery:      accountRootQuery,
		},
		Events: []*eventstore.EventToSave{{
			EventId: accountOpenedID, EventType: "AccountOpened",
			Data: `{"accountOpenedId":"018f2d5e-0001-7000-8000-000000000001","balance":0}`,
		}},
	})
	if err != nil {
		var conflict *orisun.OptimisticConcurrencyException
		if errors.As(err, &conflict) {
			log.Printf("conflict: expected=%v actual=%v", conflict.ExpectedVersion(), conflict.ActualVersion())
		}
		log.Fatal(err)
	}

	// 2. Read the latest carried state and its context position.
	latest, err := client.GetLatestByCriteria(ctx, &eventstore.GetLatestByCriteriaRequest{
		Boundary:  "accounts",
		Criteria:  accountContextQuery.Criteria,
	})
	if err != nil {
		log.Fatal(err)
	}
	expected := latest.ContextPosition // pass to the next save as ExpectedPosition

	// 3. Subscribe a projector (catch-up then live).
	handler := orisun.NewSimpleEventHandler().
		WithOnEvent(func(e *eventstore.Event) error { return nil }).
		WithOnError(func(e error) { log.Printf("subscription: %v", e) })
	sub, err := client.SubscribeToEvents(ctx, &eventstore.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "accounts",
		SubscriberName: "balance-projector",
		AfterPosition:  expected,
	}, handler)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
}
```

  </TabItem>
  <TabItem value="node" label="Node.js">

```typescript
import { EventStoreClient } from '@orisun/eventstore-client';

const client = new EventStoreClient({
  host: 'localhost',
  port: 5005,
  username: 'admin',
  password: 'changeit',
});

const accountOpenedId = '018f2d5e-0001-7000-8000-000000000001';
const accountRootQuery = {
  criteria: [{
    tags: [
      { key: 'eventType', value: 'AccountOpened' },
      { key: 'accountOpenedId', value: accountOpenedId },
    ],
  }],
};
const accountContextQuery = {
  criteria: [
    accountRootQuery.criteria[0],
    { tags: [{ key: 'scopes.accountOpenedId', value: accountOpenedId }] },
  ],
};

// 1. Open the account (context must be empty).
try {
  await client.saveEvents({
    boundary: 'accounts',
    query: {
      expectedPosition: { commitPosition: -1, preparePosition: -1 },
      subsetQuery: accountRootQuery,
    },
    events: [
      {
        eventId: accountOpenedId,
        eventType: 'AccountOpened',
        data: { accountOpenedId, balance: 0 },
      },
    ],
  });
} catch (error) {
  if (error.message.includes('AlreadyExists')) {
    // Concurrency conflict. Re-read the context and retry.
  } else {
    throw error;
  }
}

// 2. Read the latest carried state and its context position.
const latest = await client.getLatestByCriteria({
  boundary: 'accounts',
  criteria: accountContextQuery.criteria,
});
const balance = latest.results[1].event?.data.balanceAfter ?? latest.results[0].event?.data.balance ?? 0;
const expectedPosition = latest.contextPosition; // pass to the next save

// 3. Subscribe a projector (catch-up then live).
const subscription = client.subscribeToEvents(
  { subscriberName: 'balance-projector', boundary: 'accounts', afterPosition: expectedPosition },
  (event) => { /* apply event, then checkpoint event.position */ },
  (error) => console.error('subscription error:', error),
);

// subscription.cancel() to stop
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
import com.orisunlabs.orisun.client.OrisunClient;
import com.orisunlabs.orisun.client.OptimisticConcurrencyException;
import com.orisunlabs.orisun.client.EventSubscription;
import com.orisun.eventstore.Eventstore;

try (OrisunClient client = OrisunClient.newBuilder()
    .withServer("localhost", 5005)
    .withBasicAuth("admin", "changeit")
    .build()) {

  String accountOpenedId = "018f2d5e-0001-7000-8000-000000000001";
  Eventstore.Criterion accountRoot = Eventstore.Criterion.newBuilder()
      .addTags(Eventstore.Tag.newBuilder().setKey("eventType").setValue("AccountOpened").build())
      .addTags(Eventstore.Tag.newBuilder().setKey("accountOpenedId").setValue(accountOpenedId).build())
      .build();
  Eventstore.Query accountRootQuery = Eventstore.Query.newBuilder()
      .addCriteria(accountRoot)
      .build();
  Eventstore.Query accountContextQuery = Eventstore.Query.newBuilder()
      .addCriteria(accountRoot)
      .addCriteria(Eventstore.Criterion.newBuilder()
          .addTags(Eventstore.Tag.newBuilder()
              .setKey("scopes.accountOpenedId").setValue(accountOpenedId).build())
          .build())
      .build();

  // 1. Open the account (context must be empty).
  try {
      client.saveEvents(Eventstore.SaveEventsRequest.newBuilder()
          .setBoundary("accounts")
          .setQuery(Eventstore.SaveQuery.newBuilder()
              .setExpectedPosition(Eventstore.Position.newBuilder()
                  .setCommitPosition(-1).setPreparePosition(-1).build())
              .setSubsetQuery(accountRootQuery)
              .build())
          .addEvents(Eventstore.EventToSave.newBuilder()
              .setEventId(accountOpenedId)
              .setEventType("AccountOpened")
              .setData("{\"accountOpenedId\":\"018f2d5e-0001-7000-8000-000000000001\",\"balance\":0}")
              .build())
          .build());
  } catch (OptimisticConcurrencyException conflict) {
      // Concurrency conflict. Re-read the context and retry.
  }

  // 2. Read the latest carried state and its context position.
  Eventstore.GetLatestByCriteriaResponse latest = client.getLatestByCriteria(
      Eventstore.GetLatestByCriteriaRequest.newBuilder()
          .setBoundary("accounts")
          .addAllCriteria(accountContextQuery.getCriteriaList())
          .build());
  Eventstore.Position expectedPosition = latest.getContextPosition();

  // 3. Subscribe a projector (catch-up then live).
  EventSubscription sub = client.subscribeToEvents(
      Eventstore.CatchUpSubscribeToEventStoreRequest.newBuilder()
          .setBoundary("accounts")
          .setSubscriberName("balance-projector")
          .setAfterPosition(expectedPosition)
          .build(),
      new EventSubscription.EventHandler() {
          public void onEvent(Eventstore.Event event) { /* apply + checkpoint */ }
          public void onError(Throwable error) { error.printStackTrace(); }
          public void onCompleted() {}
      });

  sub.close();
}
```

  </TabItem>
  <TabItem value="grpcurl" label="grpcurl">

No install beyond the `grpcurl` binary. See the [Tutorial](../tutorial) for the same loop with `grpcurl`, or the [EventStore API](./eventstore) for each RPC.

  </TabItem>
</Tabs>

## Authenticating from a client

Every call needs credentials. The typed clients take a username and password at construction and reuse the session token the server returns on later calls, so you only supply Basic credentials once.

With `grpcurl`, send HTTP Basic in the `authorization` metadata header:

```bash
grpcurl -H 'Authorization: Basic YWRtaW46Y2hhbmdlaXQ=' localhost:5005 orisun.EventStore/Ping
```

Each authenticated response sets an `x-auth-token` header. A long-lived client can capture that token once and send it as `x-auth-token` on subsequent calls instead of re-sending Basic credentials; Orisun validates the token first and falls back to Basic. Read the full model in [Security & Authorization](../operations/security).

## Proto Files

The service definitions live in the main repository:

- [`proto/eventstore.proto`](https://github.com/OrisunLabs/Orisun/blob/main/proto/eventstore.proto)
- [`proto/admin.proto`](https://github.com/OrisunLabs/Orisun/blob/main/proto/admin.proto)

Generated Go bindings are kept in `orisun/`.

## Generate stubs for another language

If no official client exists for your language, generate stubs directly from the protobuf files with `protoc`. For example, for Python:

```bash
python -m grpc_tools.protoc \
  -I proto \
  --python_out=. --grpc_python_out=. \
  proto/eventstore.proto proto/admin.proto
```

Swap the `*_out` plugins for your target language. With gRPC reflection enabled (the default), you can also explore the API live:

```bash
grpcurl -H "$AUTH" localhost:5005 list
grpcurl -H "$AUTH" localhost:5005 describe orisun.EventStore
```

## Compatibility

Client libraries are generated from the public protobuf definitions. When upgrading Orisun, regenerate or update clients if the protobuf files changed.
