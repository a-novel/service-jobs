package dao

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobReap.sql
var jobReapQuery string

// JobReapRequest holds the parameters for a [JobReap.Exec] call.
type JobReapRequest struct {
	// Error is recorded on jobs abandoned by the sweep — those with no attempt left. It explains that
	// the lease expired, which is the only thing the reaper knows about the failure.
	Error json.RawMessage
	// RetentionDays is how long an abandoned row is kept before the purge may delete it, matching the
	// horizon a normal settle uses.
	RetentionDays int
}

// A JobReap recovers every job whose lease has expired. A job with an attempt remaining returns to
// the pending queue; a job at its attempt cap settles abandoned. Both branches happen in one
// statement, so a sweep is a single round trip regardless of how many claims lapsed.
//
// This is the correctness mechanism of the queue, not the drain: a worker that dies mid-run leaves a
// claimed row whose lease simply stops being renewed, and the reaper is what returns that work to
// circulation.
type JobReap struct{}

// NewJobReap returns a new JobReap DAO.
func NewJobReap() *JobReap {
	return new(JobReap)
}

func (dao *JobReap) Exec(ctx context.Context, request *JobReapRequest) ([]*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobReap")
	defer span.End()

	span.SetAttributes(attribute.Int("job.retention_days", request.RetentionDays))

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	entities := make([]*Job, 0)

	err = tx.NewRaw(jobReapQuery, request.Error, request.RetentionDays).Scan(ctx, &entities)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	span.SetAttributes(attribute.Int("job.reaped", len(entities)))

	return otel.ReportSuccess(span, entities), nil
}
