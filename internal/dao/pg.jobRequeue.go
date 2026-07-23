package dao

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobRequeue.sql
var jobRequeueQuery string

// ErrJobRequeueNotClaimed is returned when no job matches the requeue guard: the id is unknown, the
// job is no longer claimed, or another worker holds the claim. Like settle, it turns a stale requeue
// into a detectable no-op rather than a silent overwrite.
var ErrJobRequeueNotClaimed = errors.New("job not claimed by this worker")

// JobRequeueRequest holds the parameters for a [JobRequeue.Exec] call.
type JobRequeueRequest struct {
	ID uuid.UUID
	// WorkerID must match the claim on the row.
	WorkerID string
}

// A JobRequeue returns a claimed job to the pending queue, keeping its attempt count. It is the
// worker-driven counterpart to the lease-driven recovery the reaper performs: a worker that cannot
// finish a job it holds hands it back for another to take.
type JobRequeue struct{}

// NewJobRequeue returns a new JobRequeue DAO.
func NewJobRequeue() *JobRequeue {
	return new(JobRequeue)
}

func (dao *JobRequeue) Exec(ctx context.Context, request *JobRequeueRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobRequeue")
	defer span.End()

	span.SetAttributes(
		attribute.String("job.id", request.ID.String()),
		attribute.String("job.worker_id", request.WorkerID),
	)

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	entity := new(Job)

	err = tx.NewRaw(jobRequeueQuery, request.ID, request.WorkerID).Scan(ctx, entity)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = errors.Join(err, ErrJobRequeueNotClaimed)
		}

		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	return otel.ReportSuccess(span, entity), nil
}
