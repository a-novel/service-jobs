# Contributing to service-jobs

This file covers only what is specific to this service. For contribution conventions shared across every service — the architecture, the layers, the naming — start with the [service & architecture concepts](https://github.com/a-novel/.github/blob/master/CONTRIBUTING.md). Platform setup and day-to-day commands are in the [developer onboarding guide](https://github.com/a-novel-kit/.github/blob/master/README.md). What the service is for is in the [README](./README.md).

---

## The service is gRPC only

There is no REST server, no `openapi.yaml`, and no JavaScript client. Callers are other services on the internal network, so a second transport would be a second contract to keep in step with no consumer asking for it.

Two consequences worth knowing before you add a file:

- The container's liveness check is `grpc.health.v1.Health/Check`, not an HTTP `/ping`. A gRPC service exposes its health through the standard health protocol, so nothing here needs an HTTP listener.
- `prettier` is still installed and still runs in CI. It formats `.sql` and `.yaml`, so the migrations and the workflows are covered by `pnpm format` and gated by `pnpm lint:js` — the JavaScript toolchain survives the removal of the JavaScript client.

The `Item*Service` RPCs are placeholder wiring inherited from the service template, not a feature. Their shapes live in [`internal/models/proto/`](./internal/models/proto/), and the queue replaces them rather than growing alongside them.

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

Nothing in this service uses one yet, because no operation writes twice — the `item` resource is single-write throughout. The convention is here rather than demonstrated because wrapping a single write in a transaction is noise, and a scaffold that shows it teaches every service copied from it to do the same.

**Pass the callback's `ctx` down, not the outer one.** Data-access objects resolve their database handle from the context, and the transaction is installed on the context the callback receives. An inner call given the outer context runs on the connection pool and commits on its own, while the surrounding block still reports success. That is not hypothetical: it is what a sibling service did in four operations for months, with a green build the whole time.

Two rules follow, and the shared library's documentation is the contract for both:

- **Never call an external service inside `WithinTx`.** An open transaction holds a pooled connection for its whole lifetime; pinning one for the length of a third-party call exhausts the pool and blocks vacuuming. Persist what the call needs, close the transaction, make the call, then open a new transaction to record the result. `postgres.InTx(ctx)` reports whether a transaction is open, so a data-access object that makes an outbound call can refuse rather than rely on the convention holding.
- **A nested `WithinTx` joins the transaction in progress**, so a rollback anywhere discards the whole outermost unit of work — including work the outer caller believed was already safe. Nesting is legal; it should be deliberate. A nested call also never sees its own `sql.TxOptions`, so an operation needing a specific isolation level has to be the outermost transaction.

Unit-test a service that takes a transactor with `transactiontest.NewTransactor`, which runs the callback inline, or `NewFailingTransactor` to cover the path where the unit of work never opens — asserting the dependencies are never reached is how a test proves the writes are inside the scope rather than merely near it. A test that needs a real rollback needs a real database: use `postgres.RunDBTest`, never `RunTransactionalTest`, whose passthrough transaction cannot tell a working transactor from a broken one.

---

## Questions?

[Open an issue](https://github.com/a-novel/service-jobs/issues) — include logs and environment details.
