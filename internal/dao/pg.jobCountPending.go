package dao

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"
)

//go:embed pg.jobCountPending.sql
var jobCountPendingQuery string

// JobPendingStats is what [JobCountPending.Exec] reports: how much work is waiting, and how long the
// oldest waiting job has waited. The health endpoint reads both — a count alone cannot distinguish a
// queue draining a burst from one that has stalled.
type JobPendingStats struct {
	// Pending is the number of jobs waiting to be claimed.
	Pending int
	// OldestAge is how long the oldest pending job has been waiting. Zero when the queue is empty.
	OldestAge time.Duration
}

// A JobCountPending reports the depth of the pending queue and the age of its oldest entry.
type JobCountPending struct{}

// NewJobCountPending returns a new JobCountPending DAO.
func NewJobCountPending() *JobCountPending {
	return new(JobCountPending)
}

func (dao *JobCountPending) Exec(ctx context.Context) (*JobPendingStats, error) {
	ctx, span := otel.Tracer().Start(ctx, "dao.JobCountPending")
	defer span.End()

	tx, err := postgres.GetContext(ctx)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("get transaction: %w", err))
	}

	var row struct {
		Pending          int     `bun:"pending"`
		OldestAgeSeconds float64 `bun:"oldest_age_seconds"`
	}

	err = tx.NewRaw(jobCountPendingQuery).Scan(ctx, &row)
	if err != nil {
		return nil, otel.ReportError(span, fmt.Errorf("execute query: %w", err))
	}

	stats := &JobPendingStats{
		Pending:   row.Pending,
		OldestAge: time.Duration(row.OldestAgeSeconds * float64(time.Second)),
	}

	span.SetAttributes(
		attribute.Int("job.pending", stats.Pending),
		attribute.Float64("job.oldest_age_seconds", row.OldestAgeSeconds),
	)

	return otel.ReportSuccess(span, stats), nil
}
