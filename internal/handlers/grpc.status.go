package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/uptrace/bun"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/a-novel-kit/golib/otel"
	"github.com/a-novel-kit/golib/postgres"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// NewGrpcHealthStatus converts an error into a DependencyHealth proto message,
// mapping nil to DEPENDENCY_STATUS_UP and any non-nil error to DEPENDENCY_STATUS_DOWN.
//
// The error itself is dropped from the message: a raw dependency error routinely embeds
// internal hostnames, ports, or schema names. The health probe records it on its trace
// span, where operators can read it.
func NewGrpcHealthStatus(err error) *protogen.DependencyHealth {
	return &protogen.DependencyHealth{
		Status: lo.Ternary(
			err == nil,
			protogen.DependencyStatus_DEPENDENCY_STATUS_UP,
			protogen.DependencyStatus_DEPENDENCY_STATUS_DOWN,
		),
	}
}

// QueueDepthService measures the pending backlog reported on the status surface.
type QueueDepthService interface {
	Exec(ctx context.Context) (*core.QueueDepth, error)
}

// GrpcStatus is the gRPC handler for the Status RPC, reporting the health of the service's external
// dependencies and the queue's own backlog.
type GrpcStatus struct {
	protogen.UnimplementedStatusServiceServer

	queueDepth QueueDepthService
	cacheTTL   time.Duration

	// The backlog report is cached for cacheTTL. A health endpoint an orchestrator polls would
	// otherwise run the query on every probe against the hot path; the postgres ping stays per-call,
	// so a database that fails during the cache window is still reported down.
	mu       sync.Mutex
	cached   *core.QueueDepth
	cachedAt time.Time
}

// NewGrpcStatus returns a GrpcStatus that measures the backlog through queueDepth and caches the
// result for cacheTTL.
func NewGrpcStatus(queueDepth QueueDepthService, cacheTTL time.Duration) *GrpcStatus {
	return &GrpcStatus{queueDepth: queueDepth, cacheTTL: cacheTTL}
}

// Status probes each dependency and reports its health alongside the queue backlog.
func (handler *GrpcStatus) Status(ctx context.Context, _ *protogen.StatusRequest) (*protogen.StatusResponse, error) {
	ctx, span := otel.Tracer().Start(ctx, "grpc.Status")
	defer span.End()

	return &protogen.StatusResponse{
		Postgres: NewGrpcHealthStatus(handler.reportPostgres(ctx)),
		Queue:    handler.reportQueueDepth(ctx),
	}, nil
}

func (handler *GrpcStatus) reportPostgres(ctx context.Context) error {
	ctx, span := otel.Tracer().Start(ctx, "grpc.Status(reportPostgres)")
	defer span.End()

	pg, err := postgres.GetContext(ctx)
	if err != nil {
		return otel.ReportError(span, err)
	}

	pgdb, ok := pg.(*bun.DB)
	if !ok {
		// In transaction mode the context carries a transaction, so there is no
		// pooled connection to ping; treat the dependency as healthy.
		return nil
	}

	err = pgdb.Ping()
	if err != nil {
		return otel.ReportError(span, err)
	}

	otel.ReportSuccessNoContent(span)

	return nil
}

// reportQueueDepth returns the backlog, cached for cacheTTL. It returns nil when the measurement is
// unavailable — the database is unreachable, which reportPostgres reports as down, so the response
// carries no backlog rather than a misleading zero.
//
// The cache fields are written after construction, which agora-no-receiver-mutation forbids because
// these types are shared unsynchronised. Here they are not: mu guards every read and every write.
//
// nosemgrep: agora-no-receiver-mutation
func (handler *GrpcStatus) reportQueueDepth(ctx context.Context) *protogen.QueueDepth {
	ctx, span := otel.Tracer().Start(ctx, "grpc.Status(reportQueueDepth)")
	defer span.End()

	handler.mu.Lock()
	fresh := handler.cached != nil && time.Since(handler.cachedAt) < handler.cacheTTL
	cached := handler.cached
	handler.mu.Unlock()

	if fresh {
		return queueDepthToProto(cached)
	}

	depth, err := handler.queueDepth.Exec(ctx)
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil
	}

	handler.mu.Lock()
	handler.cached = depth
	handler.cachedAt = time.Now()
	handler.mu.Unlock()

	return queueDepthToProto(depth)
}

// queueDepthToProto maps the core backlog onto the wire message. An absent oldest-pending age (an
// empty queue) stays absent rather than becoming a zero duration.
func queueDepthToProto(depth *core.QueueDepth) *protogen.QueueDepth {
	proto := &protogen.QueueDepth{Pending: depth.Pending}
	if depth.OldestPendingAge != nil {
		proto.OldestPendingAge = durationpb.New(*depth.OldestPendingAge)
	}

	return proto
}
