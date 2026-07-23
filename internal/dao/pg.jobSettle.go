package dao

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobSettle.sql
var jobSettleQuery string

// ErrJobSettleNotClaimed is returned when no job matches the settle guard: the id is unknown, the
// job is no longer claimed, or the claim is now held by another worker. It maps a stale settle —
// one whose lease the reaper already recovered — onto a no-op the caller can detect rather than a
// silent overwrite of the newer claim.
var ErrJobSettleNotClaimed = errors.New("job not claimed by this worker")

// JobSettleRequest holds the parameters for a [JobSettle.Exec] call.
type JobSettleRequest struct {
	ID uuid.UUID
	// WorkerID must match the claim on the row. A worker cannot settle a job it does not hold.
	WorkerID string
	// Status is the terminal status to move the job to: succeeded, failed, abandoned or cancelled.
	Status JobStatus
	// Result is the handler's output on success, nil otherwise.
	Result json.RawMessage
	// Error is the structured failure on a terminal failure, nil otherwise.
	Error json.RawMessage
	// RetentionDays is how long the settled row is kept before the purge may delete it.
	RetentionDays int
}

// A JobSettle moves a claimed job to a terminal status, recording its result or error.
//
// It is guarded by the claim: a job whose lease the reaper already recovered, or that another worker
// has since claimed, matches nothing and returns [ErrJobSettleNotClaimed] rather than overwriting
// the newer state.
type JobSettle struct{}

// NewJobSettle returns a new JobSettle DAO.
func NewJobSettle() *JobSettle {
	return new(JobSettle)
}

func (dao *JobSettle) Exec(ctx context.Context, request *JobSettleRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobSettle")
	defer span.End()

	span.SetAttributes(
		attribute.String("job.id", request.ID.String()),
		attribute.String("job.worker_id", request.WorkerID),
		attribute.String("job.status", string(request.Status)),
	)

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	entity := new(Job)

	err = tx.NewRaw(
		jobSettleQuery,
		request.ID,
		request.WorkerID,
		request.Status,
		request.Result,
		request.Error,
		request.RetentionDays,
	).Scan(ctx, entity)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = errors.Join(err, ErrJobSettleNotClaimed)
		}

		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	return otel.ReportSuccess(span, entity), nil
}
