package core

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// JobCountPendingDao is the persistence dependency JobCountPending reads the queue depth from.
type JobCountPendingDao interface {
	Exec(ctx context.Context) (*dao.JobPendingStats, error)
}

// JobPendingStats reports the depth of the pending queue and the age of its oldest entry.
type JobPendingStats struct {
	Pending   int
	OldestAge time.Duration
}

// JobCountPending reports how much work is waiting and how long the oldest job has waited, which the
// health endpoint reads to tell a draining queue from a stalled one.
type JobCountPending struct {
	dao JobCountPendingDao
}

// NewJobCountPending returns a JobCountPending backed by the given DAO.
func NewJobCountPending(dao JobCountPendingDao) *JobCountPending {
	return &JobCountPending{dao: dao}
}

func (service *JobCountPending) Exec(ctx context.Context) (*JobPendingStats, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobCountPending")
	defer span.End()

	stats, err := service.dao.Exec(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("count pending jobs: %w", err))
	}

	span.SetAttributes(
		attribute.Int("job.pending", stats.Pending),
		attribute.Float64("job.oldest_age_seconds", stats.OldestAge.Seconds()),
	)

	return otel.ReportSuccess(span, &JobPendingStats{Pending: stats.Pending, OldestAge: stats.OldestAge}), nil
}
