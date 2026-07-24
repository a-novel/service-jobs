package lib

import (
	"context"
	"log"
	"time"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/core"
)

// JobReapService is the recovery sweep the loop drives. [core.JobReap] satisfies it; the loop names
// its own dependency so a test can substitute a fake without a live database.
type JobReapService interface {
	Exec(ctx context.Context) ([]*core.Job, error)
}

// A Reaper runs the recovery sweep on a fixed cadence for the life of the process. It is the loop
// every consumer would otherwise reimplement: a worker that dies mid-run leaves a claimed row whose
// lease stops being renewed, and this is what returns that work to circulation.
type Reaper struct {
	service  JobReapService
	interval time.Duration
}

// NewReaper returns a Reaper that sweeps every interval. The interval is validated at boot, so it is
// assumed positive here.
func NewReaper(service JobReapService, interval time.Duration) *Reaper {
	return &Reaper{service: service, interval: interval}
}

// Run sweeps once immediately — recovering whatever a crash stranded before this process started —
// then every interval until ctx is cancelled. It takes the boot context, never a request's: a
// request context dies at the server's own timeout, and one taken mid-transaction would hand the
// loop a transaction to sweep on.
//
// Cancellation stops the loop cleanly. A sweep is a single atomic statement, so a cancelled loop
// never leaves one half-applied; at worst an in-flight sweep rolls back and the next boot recovers
// the same jobs.
func (reaper *Reaper) Run(ctx context.Context) {
	ctx, span := otel.Tracer().Start(ctx, "lib.Reaper.Run")
	defer span.End()

	reaper.sweep(ctx)

	ticker := time.NewTicker(reaper.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reaper.sweep(ctx)
		}
	}
}

// Sweep runs a single recovery pass and reports how many jobs it recovered. Run calls it on each
// tick; it is exported so a caller can drive one deterministic pass — which is how the loop's effect
// is asserted without waiting on the ticker.
func (reaper *Reaper) Sweep(ctx context.Context) (int, error) {
	ctx, span := otel.Tracer().Start(ctx, "lib.Reaper.Sweep")
	defer span.End()

	jobs, err := reaper.service.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(jobs), nil
}

// sweep runs one pass and logs the outcome. It never returns an error: a failed sweep is logged and
// the loop lives on to try again, because a queue that stops recovering work on one transient
// database error is worse than one that retries next tick.
func (reaper *Reaper) sweep(ctx context.Context) {
	ctx, span := otel.Tracer().Start(ctx, "lib.Reaper.sweep")
	defer span.End()

	recovered, err := reaper.Sweep(ctx)
	switch {
	case err != nil:
		log.Printf("reaper: sweep failed: %v", err)
	case recovered > 0:
		log.Printf("reaper: recovered %d job(s)", recovered)
	}
}
