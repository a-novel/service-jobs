package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// JobGetDao is the persistence dependency JobGet uses to read an owner's job.
type JobGetDao interface {
	Exec(ctx context.Context, request *dao.JobGetRequest) (*dao.Job, error)
}

// JobGetRequest identifies the job to read and the owner it must belong to.
type JobGetRequest struct {
	ID uuid.UUID `validate:"required"`
	// OwnerID scopes the read. A job owned by anyone else reports not-found, so the read cannot be
	// used to learn whether a job id exists.
	OwnerID uuid.UUID `validate:"required"`
}

// JobGet reads one of an owner's jobs by its id.
type JobGet struct {
	dao JobGetDao
}

// NewJobGet returns a JobGet backed by the given DAO.
func NewJobGet(dao JobGetDao) *JobGet {
	return &JobGet{dao: dao}
}

func (service *JobGet) Exec(ctx context.Context, request *JobGetRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobGet")
	defer span.End()

	span.SetAttributes(
		attribute.String("job.id", request.ID.String()),
		attribute.String("job.owner_id", request.OwnerID.String()),
	)

	err := validate.Struct(request)
	if err != nil {
		return nil, otel.ReportError(span, errors.Join(err, ErrInvalidRequest))
	}

	entity, err := service.dao.Exec(ctx, &dao.JobGetRequest{ID: request.ID, OwnerID: request.OwnerID})
	if err != nil {
		if errors.Is(err, dao.ErrJobGetNotFound) {
			return nil, otel.ReportError(span, errors.Join(err, ErrJobNotFound))
		}

		return nil, otel.ReportError(span, fmt.Errorf("get job: %w", err))
	}

	return otel.ReportSuccess(span, jobToCore(entity)), nil
}
