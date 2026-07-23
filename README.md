# Jobs service

Durable asynchronous work for the platform: a queue that records a unit of work, hands it to a worker, and recovers whatever a crash stranded.

[![X (formerly Twitter) Follow](https://img.shields.io/twitter/follow/agorastoryverse)](https://twitter.com/agorastoryverse)
[![Discord](https://img.shields.io/discord/1315240114691248138?logo=discord)](https://discord.gg/rp4Qr8cA)

<hr />

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/a-novel/service-jobs)
![GitHub repo file or directory count](https://img.shields.io/github/directory-file-count/a-novel/service-jobs)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/a-novel/service-jobs)

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/a-novel/service-jobs/main.yaml)
[![codecov](https://codecov.io/gh/a-novel/service-jobs/graph/badge.svg)](https://codecov.io/gh/a-novel/service-jobs)

![Coverage graph](https://codecov.io/gh/a-novel/service-jobs/graphs/sunburst.svg)

## What it does

Some work takes minutes, and an HTTP request cannot wait for it. This service owns the record of that work: a caller enqueues a job, gets an identifier back immediately, and polls for the outcome. A worker in the calling service claims the job when it is ready to run it, and reports what happened.

The queue knows nothing about what the work _is_. A job carries a **kind** — a string naming the handler that runs it — and an opaque JSON payload the queue never reads. Handlers live in the services that own the domain, because a service that ran them would have to understand every domain it serves.

Two guarantees shape everything here. A job carries an **idempotency key** with no expiry, so a client retrying a request it never saw the answer to attaches to the work already in flight instead of starting a second one. And a job whose worker died is **recovered rather than lost**: its lease expires, a sweep returns it to the queue, and its recorded provider call lets a resumed run re-attach to work a third party already started rather than paying for it twice.

The surface is **gRPC only**. Callers are other services on the internal network, so there is no browser client and no REST API to keep in step with the contract. It exposes five RPCs — enqueue a job, read one, claim a batch, settle an outcome, and watch a job's state on a stream until it is terminal.

## Deploying

The service runs as published OCI images plus a PostgreSQL database. The server is stateless, so it scales to as many replicas as you need; all state lives in Postgres.

> **OpenTofu modules are the planned canonical deployment path.** Until they land, deploy the images with any container orchestrator — the composition below is the reference for which images to run, how they wire together, and the environment they expect.

| Image                          | Role                                                                        |
| ------------------------------ | --------------------------------------------------------------------------- |
| `service-jobs/grpc`            | The queue API. Internal network only.                                       |
| `service-jobs/jobs/migrations` | One-shot schema migration job; runs to completion before the server starts. |
| `service-jobs/database`        | Pre-tuned PostgreSQL image — or bring your own Postgres.                    |
| `service-jobs/standalone-grpc` | Server plus migrations in one image. Local development only.                |

Pin every image to the same release tag — see the [latest release](https://github.com/a-novel/service-jobs/releases/latest).

<!-- TODO(project-docs): replace v0.0.0 once the service cuts its first release -->

```yaml
services:
  postgres-jobs:
    image: ghcr.io/a-novel/service-jobs/database:v0.0.0
    networks: [api]
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_USER: postgres
      POSTGRES_DB: postgres
      POSTGRES_HOST_AUTH_METHOD: scram-sha-256
      POSTGRES_INITDB_ARGS: --auth=scram-sha-256
    volumes:
      - jobs-postgres-data:/var/lib/postgresql/

  migrations-jobs:
    image: ghcr.io/a-novel/service-jobs/jobs/migrations:v0.0.0
    depends_on:
      postgres-jobs: { condition: service_healthy }
    environment:
      POSTGRES_DSN: "postgres://postgres:postgres@postgres-jobs:5432/postgres?sslmode=disable"
    networks: [api]

  service-jobs:
    image: ghcr.io/a-novel/service-jobs/grpc:v0.0.0
    ports: ["${SERVICE_JOBS_GRPC_PORT}:8080"] # the container always listens on 8080
    depends_on:
      postgres-jobs: { condition: service_healthy }
      migrations-jobs: { condition: service_completed_successfully }
    environment:
      POSTGRES_DSN: "postgres://postgres:postgres@postgres-jobs:5432/postgres?sslmode=disable"
    networks: [api]

networks:
  api:

volumes:
  jobs-postgres-data:
```

### Configuration

Every variable is read from the process environment. Names can be globally prefixed with `SERVICE_JOBS_ENV_PREFIX`, which avoids collisions when another project embeds this service.

| Name           | Description                                 | Images |
| -------------- | ------------------------------------------- | ------ |
| `POSTGRES_DSN` | PostgreSQL connection string. **Required.** | all    |

<details>
<summary>Optional configuration (gRPC, reaper, connection pool, OpenTelemetry)</summary>

gRPC server:

| Name        | Description                                              | Default |
| ----------- | -------------------------------------------------------- | ------- |
| `GRPC_PORT` | Port the server listens on.                              | `8080`  |
| `GRPC_PING` | Refresh interval for the server's internal health check. | `5s`    |

Reaper — the background loop that recovers jobs a dead worker stranded (server images):

| Name              | Description                                                                                                                     | Default |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `REAPER_INTERVAL` | How often the reaper sweeps for expired leases (a Go duration, e.g. `30s`). A malformed value fails the boot, never falls back. | `30s`   |

Database connection pool (server images). The limits are **per process**, so the database's `max_connections` has to cover every replica plus the migration job; the stock `postgres` default is 100.

| Name                      | Description                               | Default |
| ------------------------- | ----------------------------------------- | ------- |
| `POSTGRES_MAX_OPEN_CONNS` | Maximum open connections to the database. | `20`    |
| `POSTGRES_MAX_IDLE_CONNS` | Maximum connections kept open while idle. | `20`    |

Logs and tracing — OpenTelemetry supports a stdout and a Google Cloud exporter (all server images):

| Name                | Description                                                           | Default        |
| ------------------- | --------------------------------------------------------------------- | -------------- |
| `OTEL`              | Enable OTel tracing; the variables below pick the exporter.           | `false`        |
| `GCLOUD_PROJECT_ID` | Google Cloud project ID. When set, switches the OTel exporter to GCP. |                |
| `APP_NAME`          | Application name attached to traces and logs.                         | `service-jobs` |

</details>

## Using the client package

The Go client is what a consuming service imports. The snippet below is the **minimum viable call**; the full surface is what your editor's intellisense and [pkg.go.dev](https://pkg.go.dev/github.com/a-novel/service-jobs) are for.

```bash
go get github.com/a-novel/service-jobs
```

```go
package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	servicejobs "github.com/a-novel/service-jobs/pkg/go"
)

func main() {
	ctx := context.Background()

	// In production, swap insecure.NewCredentials() for a TLS or mTLS credential — the
	// server has no application-layer auth, so transport security is the only thing
	// protecting it from a network adversary.
	client, err := servicejobs.NewClient(
		"service-jobs:8080",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	requestKey := "a-request-key" // unique per submission; a retry reuses it

	// Enqueue a job. The idempotency key makes a retry of this call attach to the same
	// job rather than start a second one; created is false when that happens.
	resp, err := client.JobEnqueue(ctx, &servicejobs.JobEnqueueRequest{
		Kind:           "generate",
		Payload:        []byte(`{"seed":"an idea"}`),
		OwnerId:        "00000000-0000-0000-0000-000000000001", // the end user this job acts for
		IdempotencyKey: &requestKey,
		MaxAttempts:    1,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("job %s created=%t", resp.GetJob().GetId(), resp.GetCreated())
}
```

## Reaper cost

The reaper — the background loop that recovers jobs a dead worker stranded — sweeps on a fixed interval (30s by default). A sweep is one statement served by a partial index on claimed rows, so an idle queue costs an index probe, and a real sweep's cost tracks how many leases actually lapsed, not the size of the queue around them.

The `benchmark-go` CI job measures one sweep recovering a given number of stranded claims. On GitHub's standard 4-core runner:

| Claims recovered in one sweep | Reap latency |
| ----------------------------- | ------------ |
| 100                           | ~1.5 ms      |
| 1,000                         | ~13 ms       |
| 10,000                        | ~240 ms      |

Even a whole worker fleet dying at once — 10,000 claims stranded — is recovered in well under a second against a 30-second sweep interval, so the reaper spends a fraction of a percent of its cycle even in that worst case and is never the queue's capacity limit. (Much of the 10,000-claim figure is materializing every recovered row back to the caller, which the loop then only counts, so it is a ceiling rather than the cost of the recovery itself.)

Reproduce it against any Postgres:

```bash
POSTGRES_DSN=... go test -bench=BenchmarkJobReap -benchmem -benchtime=20x -run='^$' ./internal/dao/...
```

## Running locally

The `standalone-grpc` image bundles the migration job with the server, so a single container brings the service up against an empty database. It is a development convenience: a production deployment runs migrations as their own job, so a failed migration stops the rollout instead of restarting a server.

```bash
a-novel run start service-jobs/grpc
eval "$(a-novel run env service-jobs)"

grpcurl --plaintext localhost:${SERVICE_JOBS_GRPC_PORT} list
```

Working on the code itself starts with [CONTRIBUTING.md](./CONTRIBUTING.md).

## Contributing

Platform setup and the day-to-day commands live in the [developer onboarding guide](https://github.com/a-novel-kit/.github/blob/master/README.md). What is specific to this service is in [CONTRIBUTING.md](./CONTRIBUTING.md).
