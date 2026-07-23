# Contributing to service-jobs

This file covers only what is specific to this service. For contribution conventions shared across every service — the architecture, the layers, the naming — start with the [service & architecture concepts](https://github.com/a-novel/.github/blob/master/CONTRIBUTING.md). Platform setup and day-to-day commands are in the [developer onboarding guide](https://github.com/a-novel-kit/.github/blob/master/README.md). What the service is for is in the [README](./README.md).

---

## The service is gRPC only

There is no REST server, no `openapi.yaml`, and no JavaScript client. Callers are other services on the internal network, so a second transport would be a second contract to keep in step with no consumer asking for it.

Two consequences worth knowing before you add a file:

- The container's liveness check is `grpc.health.v1.Health/Check`, not an HTTP `/ping`. A gRPC service exposes its health through the standard health protocol, so nothing here needs an HTTP listener.
- `prettier` is still installed and still runs in CI. It formats `.sql` and `.yaml`, so the migrations and the workflows are covered by `pnpm format` and gated by `pnpm lint:js` — the JavaScript toolchain survives the removal of the JavaScript client.

The queue is exposed as five per-operation services — `JobEnqueueService`, `JobGetService`, `JobClaimService`, `JobSettleService` and the streaming `JobWatchService` — whose contracts live in [`internal/models/proto/`](./internal/models/proto/). `JobWatch` is server-streaming; the rest are unary.

---

## Running it locally

Start the server and load its ports into your shell:

```bash
a-novel run start service-jobs/grpc
eval "$(a-novel run env service-jobs)"
```

Check it is alive:

```bash
grpcurl -plaintext localhost:${SERVICE_JOBS_GRPC_PORT} list
grpcurl -plaintext localhost:${SERVICE_JOBS_GRPC_PORT} StatusService/Status
```

---

## Transactions

Two or more writes that must land together are wrapped in a `transaction.Transactor`, taken as a constructor dependency by the service that needs one and injected in `cmd`. It names no database, so business code says "these writes are one unit" without knowing what stores them:

```go
// internal/core
type SomeService struct {
	dao        SomeServiceDao
	transactor transaction.Transactor
}

err := service.transactor.WithinTx(ctx, func(ctx context.Context) error {
	// every data-access call made with this ctx is part of one transaction
})

// cmd
service := core.NewSomeService(daoSomething, postgres.NewTransactor(nil))
```

The settle service is the one place that uses it. A retryable failure reads the job's attempt count and then either requeues it or gives up — a write conditioned on a read — and the transaction is what stops the reaper recovering the job between the two. Every other operation is a single write and takes no transaction.

**Pass the callback's `ctx` down, not the outer one.** Data-access objects resolve their database handle from the context, and the transaction is installed on the context the callback receives. An inner call given the outer context runs on the connection pool and commits on its own, while the surrounding block still reports success. That is not hypothetical: it is what a sibling service did in four operations for months, with a green build the whole time.

Two rules follow, and the shared library's documentation is the contract for both:

- **Never call an external service inside `WithinTx`.** An open transaction holds a pooled connection for its whole lifetime; pinning one for the length of a third-party call exhausts the pool and blocks vacuuming. Persist what the call needs, close the transaction, make the call, then open a new transaction to record the result. `postgres.InTx(ctx)` reports whether a transaction is open, so a data-access object that makes an outbound call can refuse rather than rely on the convention holding.
- **A nested `WithinTx` joins the transaction in progress**, so a rollback anywhere discards the whole outermost unit of work — including work the outer caller believed was already safe. Nesting is legal; it should be deliberate. A nested call also never sees its own `sql.TxOptions`, so an operation needing a specific isolation level has to be the outermost transaction.

Unit-test a service that takes a transactor with `transactiontest.NewTransactor`, which runs the callback inline, or `NewFailingTransactor` to cover the path where the unit of work never opens — asserting the dependencies are never reached is how a test proves the writes are inside the scope rather than merely near it. A test that needs a real rollback needs a real database: use `postgres.RunDBTest`, never `RunTransactionalTest`, whose passthrough transaction cannot tell a working transactor from a broken one.

---

## Schema conventions

These hold for every new table, as the `jobs` table demonstrates.

**Identifiers are time-ordered and minted in Go.** Columns default to `uuidv7()`, which the project's PostgreSQL 18 image provides natively, so a table under insert churn keeps index locality instead of scattering writes across the whole B-tree. Core still generates the identifier and passes it in: an insert that has to read back a database-generated id cannot tell its own row from one a concurrent caller inserted under the same unique key.

**Timestamps are full precision, and the database is the clock.** Declare `timestamptz`, never `timestamp(0)` — second precision cannot order two commits, let alone express a lease expiry. Default them to `clock_timestamp()`, never `now()` or `CURRENT_TIMESTAMP`: those two are frozen at transaction start, so a column written inside a transaction can never advance past a value its neighbors already hold. Several workers compare these timestamps, so the database has to be the single clock, or application-server skew enters the arithmetic.

**Owned rows carry an `owner_id`, with no cross-service foreign key.** Identity belongs to another service, so there is nothing local to reference and no constraint to declare.

**The value arrives from the caller, and that is a deliberate boundary.** This service authenticates nobody: it is internal and unreachable from outside, the same posture `service-json-keys` takes while serving private key material. The calling service verified its own user and is trusted to pass the right owner. What that buys, and what it does not:

- The predicate **still** stops one user's row reaching another through a caller's own bug. A read that omits the owner returns no rows rather than somebody else's job, and that is the mistake a handler under time pressure actually makes.
- It **no longer** defends against a compromised caller, which could pass any owner it liked. The network boundary owns that.

**Ownership is a query predicate, not a check after the fact:**

```sql
SELECT
  *
FROM
  jobs
WHERE
  id = ?0
  AND owner_id = ?1;
```

A predicate is fail-closed: a caller that forgets the owner argument fails to scan rather than returning someone else's row, whereas a later `if row.OwnerID != actor.UserID` is one early return away from being skipped. It also collapses "no such row" and "not your row" into one no-rows result, which removes an existence oracle over a priced resource at no cost.

**A cross-owner read is not-found, never access-denied.** The data-access object joins `sql.ErrNoRows` onto its own sentinel, and the handler maps that to `NOT_FOUND`. Answering `PERMISSION_DENIED` would confirm the row exists.

**Every migration gets its own deliberately allocated prefix.** Take it from `date '+%Y%m%d%H%M%S'` at the moment you create the file. bun derives a migration's identity from that numeric prefix alone, so two files sharing one merge into a single migration: the second replaces the first, with no error at discovery and none at apply, and the first migration simply never runs. An `.up.sql` and its `.down.sql` are meant to share a prefix; two different migrations are not.

---

## Questions?

[Open an issue](https://github.com/a-novel/service-jobs/issues) — include logs and environment details.
