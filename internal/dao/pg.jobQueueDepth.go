package dao

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobQueueDepth.sql
var jobQueueDepthQuery string

// QueueDepth is the backlog measurement: the number of due-and-unclaimed jobs, and the age of the
// oldest of them. OldestPendingAge is nil when nothing is pending — an absent age, not a zero one.
type QueueDepth struct {
	Pending          int64
	OldestPendingAge *time.Duration
}

// A JobQueueDepth measures the pending backlog in one query. It reads only the pending partition
// through jobs_dispatch_idx, so an idle or shallow queue costs an index probe rather than a scan of
// the terminal rows around it.
type JobQueueDepth struct{}

// NewJobQueueDepth returns a new JobQueueDepth DAO.
func NewJobQueueDepth() *JobQueueDepth {
	return new(JobQueueDepth)
}

func (dao *JobQueueDepth) Exec(ctx context.Context) (*QueueDepth, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobQueueDepth")
	defer span.End()

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	// count(*) is never null; min(run_at) — and so the age — is null when the pending partition is
	// empty, which is a nullable scan target rather than a coalesced zero.
	var row struct {
		Pending          int64           `bun:"pending"`
		OldestPendingAge sql.NullFloat64 `bun:"oldest_pending_age"`
	}

	err = tx.NewRaw(jobQueueDepthQuery).Scan(ctx, &row)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	depth := &QueueDepth{Pending: row.Pending}
	if row.OldestPendingAge.Valid {
		age := time.Duration(row.OldestPendingAge.Float64 * float64(time.Second))
		depth.OldestPendingAge = &age
	}

	span.SetAttributes(attribute.Int64("job.pending", depth.Pending))

	return otel.ReportSuccess(span, depth), nil
}
