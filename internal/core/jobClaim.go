package core

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// JobClaimDao is the persistence dependency JobClaim uses to take a batch of jobs.
type JobClaimDao interface {
	Exec(ctx context.Context, request *dao.JobClaimRequest) ([]*dao.Job, error)
}

// JobClaimRequest carries what a worker needs to claim work.
type JobClaimRequest struct {
	// Kinds are the handler kinds this worker serves. A worker that names none claims nothing.
	Kinds []string
	// WorkerID is recorded on each claimed job, so a stranded claim can be traced.
	WorkerID string `validate:"required"`
	// Limit caps how many jobs one claim takes.
	Limit int `validate:"gt=0"`
	// LeaseSeconds is the visibility timeout: how long the claim holds before the reaper may recover
	// the job.
	LeaseSeconds int `validate:"gt=0"`
}

// JobClaim takes a batch of pending jobs for a worker to run.
type JobClaim struct {
	dao JobClaimDao
}

// NewJobClaim returns a JobClaim backed by the given DAO.
func NewJobClaim(dao JobClaimDao) *JobClaim {
	return &JobClaim{dao: dao}
}

func (service *JobClaim) Exec(ctx context.Context, request *JobClaimRequest) ([]*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobClaim")
	defer span.End()

	span.SetAttributes(
		attribute.StringSlice("job.kinds", request.Kinds),
		attribute.String("job.worker_id", request.WorkerID),
		attribute.Int("job.limit", request.Limit),
	)

	err := validate.Struct(request)
	if err != nil {
		return nil, otel.ReportError(span, errors.Join(err, ErrInvalidRequest))
	}

	entities, err := service.dao.Exec(ctx, &dao.JobClaimRequest{
		Kinds:        request.Kinds,
		WorkerID:     request.WorkerID,
		Limit:        request.Limit,
		LeaseSeconds: request.LeaseSeconds,
	})
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("claim jobs: %w", err))
	}

	jobs := make([]*Job, len(entities))
	for i, entity := range entities {
		jobs[i] = newJob(entity)
	}

	span.SetAttributes(attribute.Int("job.claimed", len(jobs)))

	return otel.ReportSuccess(span, jobs), nil
}
