package core

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/a-novel/service-jobs/internal/dao"
)

// Core-layer sentinels the handlers map onto transport status codes. They are the vocabulary the
// business layer speaks, translated from the data-access sentinels so a handler never imports dao.
var (
	// ErrJobNotFound is returned when a requested job does not exist, or is not visible to the
	// caller. A cross-owner read reports this rather than a distinct access error, so the response
	// cannot be used to probe for job ids.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobConflict is returned when an enqueue reuses an idempotency key for a different request —
	// the same owner and kind and key, but a different payload. It is a reused key, not a replay.
	ErrJobConflict = errors.New("idempotency key reused for a different request")
	// ErrJobNotClaimed is returned when a worker acts on a job it does not hold: the job was never
	// claimed by it, or its claim has since been recovered by the reaper or taken by another worker.
	ErrJobNotClaimed = errors.New("job not claimed by this worker")
)

// A JobStatus is where a job sits in its lifecycle, re-exported from the data-access layer so
// handlers depend on the core vocabulary alone.
type JobStatus = dao.JobStatus

// The lifecycle statuses, re-exported for callers that branch on them.
const (
	JobStatusPending   = dao.JobStatusPending
	JobStatusClaimed   = dao.JobStatusClaimed
	JobStatusSucceeded = dao.JobStatusSucceeded
	JobStatusFailed    = dao.JobStatusFailed
	JobStatusAbandoned = dao.JobStatusAbandoned
	JobStatusCancelled = dao.JobStatusCancelled
)

// A Job is one unit of asynchronous work as the business layer sees it. It is the data-access row
// with the columns a caller has no use for — the request fingerprint, the internal lease bookkeeping
// — left behind.
type Job struct {
	ID      uuid.UUID
	Kind    string
	Payload json.RawMessage
	OwnerID uuid.UUID

	Status      JobStatus
	Attempt     int16
	MaxAttempts int16

	Result json.RawMessage
	Error  json.RawMessage

	ProviderCallID *string

	CreatedAt time.Time
	UpdatedAt time.Time
	SettledAt *time.Time
	ExpiresAt *time.Time
}

// newJob maps a data-access row onto the core view.
func newJob(entity *dao.Job) *Job {
	return &Job{
		ID:             entity.ID,
		Kind:           entity.Kind,
		Payload:        entity.Payload,
		OwnerID:        entity.OwnerID,
		Status:         entity.Status,
		Attempt:        entity.Attempt,
		MaxAttempts:    entity.MaxAttempts,
		Result:         entity.Result,
		Error:          entity.Error,
		ProviderCallID: entity.ProviderCallID,
		CreatedAt:      entity.CreatedAt,
		UpdatedAt:      entity.UpdatedAt,
		SettledAt:      entity.SettledAt,
		ExpiresAt:      entity.ExpiresAt,
	}
}
