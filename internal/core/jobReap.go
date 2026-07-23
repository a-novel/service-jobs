package core

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// jobReapError is recorded on every job the sweep abandons. The reaper knows only that the lease
// lapsed — a worker that wants a richer cause has to settle the job itself before the lease expires —
// so that is all this can say.
var jobReapError = json.RawMessage(`{"reason":"lease expired"}`)

// JobReapDao is the persistence dependency JobReap uses to recover lapsed claims.
type JobReapDao interface {
	Exec(ctx context.Context, request *dao.JobReapRequest) ([]*dao.Job, error)
}

// JobReap recovers every job whose lease has expired: one with an attempt left returns to pending,
// one at its cap settles abandoned. The requeue-or-abandon decision lives in the DAO's single
// statement; this service supplies the abandon error and the retention horizon, then maps the result
// to the core view. It takes no per-call parameters — a sweep recovers whatever has lapsed.
type JobReap struct {
	dao           JobReapDao
	retentionDays int
}

// NewJobReap returns a JobReap that abandons to the given retention horizon — the same one a settle
// uses, so a reaper-abandoned row and a worker-failed row age out of the table together.
func NewJobReap(dao JobReapDao, retentionDays int) *JobReap {
	return &JobReap{dao: dao, retentionDays: retentionDays}
}

func (service *JobReap) Exec(ctx context.Context) ([]*Job, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobReap")
	defer span.End()

	entities, err := service.dao.Exec(ctx, &dao.JobReapRequest{
		Error:         jobReapError,
		RetentionDays: service.retentionDays,
	})
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("reap jobs: %w", err))
	}

	jobs := make([]*Job, len(entities))
	for i, entity := range entities {
		jobs[i] = jobToCore(entity)
	}

	span.SetAttributes(attribute.Int("job.reaped", len(jobs)))

	return otel.ReportSuccess(span, jobs), nil
}
