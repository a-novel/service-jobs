package dao

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobClaim.sql
var jobClaimQuery string

// JobClaimRequest holds the parameters for a [JobClaim.Exec] call.
type JobClaimRequest struct {
	// Kinds are the handler kinds this worker serves. A worker only claims work it can run, so the
	// claim is scoped to the kinds it registered handlers for.
	Kinds []string
	// WorkerID is recorded on each claimed job as claimed_by, so a stranded claim can be traced to
	// the worker that took it.
	WorkerID string
	// Limit caps how many jobs one claim takes.
	Limit int
	// LeaseSeconds is the visibility timeout: how long the claim is valid before the reaper may
	// recover the job. It is added to the database clock, not the worker's, so skew never reaches
	// the lease.
	LeaseSeconds int
}

// A JobClaim takes a batch of pending jobs for a worker to run.
//
// Concurrent claims never contend: the claimable rows are locked with SKIP LOCKED, so two workers
// polling at the same moment take disjoint batches and neither blocks. A claim that finds nothing
// returns an empty slice rather than an error.
type JobClaim struct{}

// NewJobClaim returns a new JobClaim DAO.
func NewJobClaim() *JobClaim {
	return new(JobClaim)
}

func (dao *JobClaim) Exec(ctx context.Context, request *JobClaimRequest) ([]*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobClaim")
	defer span.End()

	span.SetAttributes(
		attribute.StringSlice("job.kinds", request.Kinds),
		attribute.String("job.worker_id", request.WorkerID),
		attribute.Int("job.limit", request.Limit),
		attribute.Int("job.lease_seconds", request.LeaseSeconds),
	)

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	entities := make([]*Job, 0, request.Limit)

	err = tx.NewRaw(
		jobClaimQuery,
		bun.List(request.Kinds),
		request.Limit,
		request.WorkerID,
		request.LeaseSeconds,
	).Scan(ctx, &entities)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	return otel.ReportSuccess(span, entities), nil
}
