package core

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/dao"
)

// QueueDepth is the queue's backlog as the business layer sees it: the number of due-and-unclaimed
// jobs and the age of the oldest of them. OldestPendingAge is nil when the queue is empty.
type QueueDepth struct {
	Pending          int64
	OldestPendingAge *time.Duration
}

// JobQueueDepthDao is the persistence dependency JobQueueDepth measures through.
type JobQueueDepthDao interface {
	Exec(ctx context.Context) (*dao.QueueDepth, error)
}

// JobQueueDepth measures the pending backlog. It is the status surface's window into the queue: a
// count alone cannot tell a queue absorbing a burst from a stalled one, so it reports the age of the
// oldest waiting job beside the count.
type JobQueueDepth struct {
	dao JobQueueDepthDao
}

// NewJobQueueDepth returns a JobQueueDepth backed by the given DAO.
func NewJobQueueDepth(dao JobQueueDepthDao) *JobQueueDepth {
	return &JobQueueDepth{dao: dao}
}

func (service *JobQueueDepth) Exec(ctx context.Context) (*QueueDepth, error) {
	ctx, span := otel.Tracer().Start(ctx, "service.JobQueueDepth")
	defer span.End()

	depth, err := service.dao.Exec(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("measure queue depth: %w", err))
	}

	span.SetAttributes(attribute.Int64("job.pending", depth.Pending))

	return otel.ReportSuccess(span, &QueueDepth{
		Pending:          depth.Pending,
		OldestPendingAge: depth.OldestPendingAge,
	}), nil
}
