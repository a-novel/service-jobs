package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// JobEnqueueDao is the persistence dependency JobEnqueue uses to record a job.
type JobEnqueueDao interface {
	Exec(ctx context.Context, request *dao.JobEnqueueRequest) (*dao.Job, error)
}

// JobEnqueueRequest carries the fields for a new job.
type JobEnqueueRequest struct {
	// Kind selects the handler that will run the job. Required.
	Kind string `validate:"required,notblank"`
	// Payload is the handler's opaque input. It must be valid JSON.
	Payload json.RawMessage
	// OwnerID is the user the job acts for, supplied by the calling service.
	OwnerID uuid.UUID `validate:"required"`
	// IdempotencyKey deduplicates repeat submissions within one owner and kind. Nil skips
	// deduplication.
	IdempotencyKey *string
	// MaxAttempts caps the runs the job gets. Defaults to one when zero, the right floor for a
	// priced call.
	MaxAttempts int16 `validate:"gte=0"`
}

// A JobEnqueueResult is what an enqueue reports back: the job, and whether this call created it.
type JobEnqueueResult struct {
	Job *Job
	// Created is false when an existing job was returned under the same idempotency key, so a caller
	// retrying a request attaches to the work already in flight instead of paying for a second one.
	Created bool
}

// JobEnqueue records a unit of asynchronous work, deduplicating on the idempotency key.
type JobEnqueue struct {
	dao JobEnqueueDao
}

// NewJobEnqueue returns a JobEnqueue backed by the given DAO.
func NewJobEnqueue(dao JobEnqueueDao) *JobEnqueue {
	return &JobEnqueue{dao: dao}
}

func (service *JobEnqueue) Exec(ctx context.Context, request *JobEnqueueRequest) (*JobEnqueueResult, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobEnqueue")
	defer span.End()

	span.SetAttributes(
		attribute.String("job.kind", request.Kind),
		attribute.String("job.owner_id", request.OwnerID.String()),
		attribute.Bool("job.idempotent", request.IdempotencyKey != nil),
	)

	err := validate.Struct(request)
	if err != nil {
		return nil, otel.ReportError(span, errors.Join(err, ErrInvalidRequest))
	}

	// The id is minted here, not defaulted by the database, so the winner of an idempotency race is
	// the row whose returned id matches the one this call generated. A computed discriminator cannot
	// stand in: bun discards a returned column with no matching struct field, so a RETURNING flag is
	// dropped silently and every insert scans as a duplicate.
	id, err := uuid.NewV7()
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("generate id: %w", err))
	}

	maxAttempts := request.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 1
	}

	fingerprint := fingerprintOf(request.Payload)

	entity, err := service.dao.Exec(ctx, &dao.JobEnqueueRequest{
		ID:                 id,
		Kind:               request.Kind,
		Payload:            payloadOr(request.Payload),
		OwnerID:            request.OwnerID,
		IdempotencyKey:     request.IdempotencyKey,
		RequestFingerprint: fingerprint,
		MaxAttempts:        maxAttempts,
	})
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("enqueue job: %w", err))
	}

	created := entity.ID == id

	// A returned job under the same key but with a different fingerprint means the key was reused for
	// different work, which is a conflict rather than a replay: the caller must not silently receive
	// a job that does not match the payload it sent.
	if !created && !bytes.Equal(entity.RequestFingerprint, fingerprint) {
		return nil, otel.ReportError(span, ErrJobConflict)
	}

	span.SetAttributes(attribute.Bool("job.created", created))

	return otel.ReportSuccess(span, &JobEnqueueResult{Job: newJob(entity), Created: created}), nil
}

// fingerprintOf digests the exact request bytes. It hashes the raw payload rather than a
// canonicalized form: a client retry replays identical bytes, so a byte digest is a safe conflict
// signal, and re-ordered but equivalent JSON reporting as a conflict is a harmless false positive.
func fingerprintOf(payload json.RawMessage) []byte {
	sum := sha256.Sum256(payloadOr(payload))

	return sum[:]
}

// payloadOr returns an empty JSON object for a nil payload, matching the column's own default so the
// stored value and the fingerprinted value agree.
func payloadOr(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}

	return payload
}
