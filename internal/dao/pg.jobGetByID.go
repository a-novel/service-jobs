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

//go:embed pg.jobGetByID.sql
var jobGetByIDQuery string

// ErrJobGetByIDNotFound is returned when no job has the requested id.
var ErrJobGetByIDNotFound = errors.New("job not found")

// JobGetByIDRequest holds the parameters for a [JobGetByID.Exec] call.
type JobGetByIDRequest struct {
	ID uuid.UUID
}

// A JobGetByID reads a job by its id, without an owner scope. It serves the worker-side path: the
// settle service reads a job it is about to finish to decide, from its attempt count, whether a
// retryable failure requeues or gives up. Owner-scoped reads use [JobGet] instead.
type JobGetByID struct{}

// NewJobGetByID returns a new JobGetByID DAO.
func NewJobGetByID() *JobGetByID {
	return new(JobGetByID)
}

func (dao *JobGetByID) Exec(ctx context.Context, request *JobGetByIDRequest) (*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobGetByID")
	defer span.End()

	span.SetAttributes(attribute.String("job.id", request.ID.String()))

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	entity := new(Job)

	err = tx.NewRaw(jobGetByIDQuery, request.ID).Scan(ctx, entity)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = errors.Join(err, ErrJobGetByIDNotFound)
		}

		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	return otel.ReportSuccess(span, entity), nil
}
