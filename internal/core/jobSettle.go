package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/transaction"

	"github.com/a-novel/service-jobs/internal/dao"
)

// JobSettleSettleDao is the terminal-transition dependency: it moves a claimed job to a terminal
// status. Named for the DAO it wraps to keep the two settle concepts — the service and the
// primitive — distinct.
type JobSettleSettleDao interface {
	Exec(ctx context.Context, request *dao.JobSettleRequest) (*dao.Job, error)
}

// JobSettleRequeueDao returns a claimed job to the pending queue.
type JobSettleRequeueDao interface {
	Exec(ctx context.Context, request *dao.JobRequeueRequest) (*dao.Job, error)
}

// JobSettleGetDao reads a job by id, so the retry path can weigh its attempts before deciding.
type JobSettleGetDao interface {
	Exec(ctx context.Context, request *dao.JobGetByIDRequest) (*dao.Job, error)
}

// JobFailure is the outcome of a failed run.
type JobFailure struct {
	// Error is the structured failure recorded on the job.
	Error json.RawMessage
	// Retryable marks a failure the substrate may retry while attempts remain. A worker that does not
	// set it gets a single attempt, the right default for a priced call.
	Retryable bool
}

// JobSettleRequest reports the outcome of a run. Exactly one of Result and Failure is set: a nil
// Failure is a success carrying Result, a non-nil Failure is a failed run.
type JobSettleRequest struct {
	ID uuid.UUID `validate:"required"`
	// WorkerID must hold the claim on the job. A worker cannot settle work it does not hold.
	WorkerID string `validate:"required"`
	// Result is the handler's output on success.
	Result json.RawMessage
	// Failure describes a failed run, or is nil on success.
	Failure *JobFailure
}

// JobSettle records the outcome of a run.
//
// A success or a terminal failure is one write, guarded by the claim. A retryable failure is two
// steps — read the attempts, then requeue if any remain or give up if none do — and those run inside
// a transaction so the reaper cannot recover the job between the read and the write. That is the one
// operation in this service that writes conditionally on what it read, and the transactor is what
// makes it atomic.
type JobSettle struct {
	settleDao  JobSettleSettleDao
	requeueDao JobSettleRequeueDao
	getDao     JobSettleGetDao
	transactor transaction.Transactor
	// retentionDays is how long a settled row is kept before the purge may delete it.
	retentionDays int
}

// NewJobSettle returns a JobSettle. The transactor is injected so the retry path's read-then-write is
// one unit; cmd supplies the PostgreSQL one.
func NewJobSettle(
	settleDao JobSettleSettleDao,
	requeueDao JobSettleRequeueDao,
	getDao JobSettleGetDao,
	transactor transaction.Transactor,
	retentionDays int,
) *JobSettle {
	return &JobSettle{
		settleDao:     settleDao,
		requeueDao:    requeueDao,
		getDao:        getDao,
		transactor:    transactor,
		retentionDays: retentionDays,
	}
}

func (service *JobSettle) Exec(ctx context.Context, request *JobSettleRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobSettle")
	defer span.End()

	span.SetAttributes(
		attribute.String("job.id", request.ID.String()),
		attribute.String("job.worker_id", request.WorkerID),
		attribute.Bool("job.failed", request.Failure != nil),
	)

	err := validate.Struct(request)
	if err != nil {
		return nil, otel.ReportError(span, errors.Join(err, ErrInvalidRequest))
	}

	// Success, and failure with no retry, are single terminal writes. The DAO's own claim guard
	// rejects a stale settle, so neither needs the transaction the retry path does.
	if request.Failure == nil {
		return service.settle(ctx, request, dao.JobStatusSucceeded, request.Result, nil)
	}

	if !request.Failure.Retryable {
		return service.settle(ctx, request, dao.JobStatusFailed, nil, request.Failure.Error)
	}

	return service.retry(ctx, request)
}

// settle performs one terminal transition and maps the not-claimed sentinel.
func (service *JobSettle) settle(
	ctx context.Context, request *JobSettleRequest,
	status dao.JobStatus, result, jobErr json.RawMessage,
) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "core.JobSettle(settle)")
	defer span.End()

	entity, err := service.settleDao.Exec(ctx, &dao.JobSettleRequest{
		ID:            request.ID,
		WorkerID:      request.WorkerID,
		Status:        status,
		Result:        result,
		Error:         jobErr,
		RetentionDays: service.retentionDays,
	})
	if err != nil {
		if errors.Is(err, dao.ErrJobSettleNotClaimed) {
			return nil, otel.ReportError(span, errors.Join(err, ErrJobNotClaimed))
		}

		return nil, otel.ReportError(span, fmt.Errorf("settle job: %w", err))
	}

	return otel.ReportSuccess(span, jobToCore(entity)), nil
}

// retry decides a retryable failure: requeue while an attempt remains, give up when none does. The
// read and the write are one transaction, so the reaper cannot recover the job in between and turn
// the decision stale under it.
func (service *JobSettle) retry(ctx context.Context, request *JobSettleRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "core.JobSettle(retry)")
	defer span.End()

	var result *Job

	err := service.transactor.WithinTx(ctx, func(ctx context.Context) error {
		entity, err := service.getDao.Exec(ctx, &dao.JobGetByIDRequest{ID: request.ID})
		if err != nil {
			if errors.Is(err, dao.ErrJobGetByIDNotFound) {
				return errors.Join(err, ErrJobNotClaimed)
			}

			return fmt.Errorf("read job: %w", err)
		}

		// A job the worker no longer holds — recovered by the reaper, or taken by another worker — is
		// not this worker's to settle.
		if entity.Status != dao.JobStatusClaimed || entity.ClaimedBy == nil || *entity.ClaimedBy != request.WorkerID {
			return ErrJobNotClaimed
		}

		if entity.Attempt < entity.MaxAttempts {
			requeued, err := service.requeueDao.Exec(ctx, &dao.JobRequeueRequest{
				ID:       request.ID,
				WorkerID: request.WorkerID,
			})
			if err != nil {
				return fmt.Errorf("requeue job: %w", err)
			}

			result = jobToCore(requeued)

			return nil
		}

		settled, err := service.settleDao.Exec(ctx, &dao.JobSettleRequest{
			ID:            request.ID,
			WorkerID:      request.WorkerID,
			Status:        dao.JobStatusFailed,
			Error:         request.Failure.Error,
			RetentionDays: service.retentionDays,
		})
		if err != nil {
			return fmt.Errorf("settle job: %w", err)
		}

		result = jobToCore(settled)

		return nil
	})
	if err != nil {
		return nil, otel.ReportError(span, err)
	}

	return otel.ReportSuccess(span, result), nil
}
