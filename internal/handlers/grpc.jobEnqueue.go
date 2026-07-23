package handlers

import (
	"context"
	"math"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// JobEnqueueService records a job and reports whether this call created it. The core layer supplies
// the implementation.
type JobEnqueueService interface {
	Exec(ctx context.Context, request *core.JobEnqueueRequest) (*core.JobEnqueueResult, error)
}

// JobEnqueue is the gRPC handler for the JobEnqueue RPC.
type JobEnqueue struct {
	protogen.UnimplementedJobEnqueueServiceServer

	service JobEnqueueService
}

// NewJobEnqueue returns a new JobEnqueue handler.
func NewJobEnqueue(service JobEnqueueService) *JobEnqueue {
	return &JobEnqueue{service: service}
}

func (handler *JobEnqueue) JobEnqueue(
	ctx context.Context, request *protogen.JobEnqueueRequest,
) (*protogen.JobEnqueueResponse, error) {
	ctx, span := otel.Tracer().Start(ctx, "grpc.JobEnqueue")
	defer span.End()

	ownerID, err := uuid.Parse(request.GetOwnerId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, status.Error(codes.InvalidArgument, "invalid owner id")
	}

	// The wire carries max_attempts as int32; the column is a smallint. A value outside the smallint
	// range is nonsensical for an attempt cap, so it is rejected rather than silently truncated.
	if request.GetMaxAttempts() < 0 || request.GetMaxAttempts() > math.MaxInt16 {
		return nil, status.Error(codes.InvalidArgument, "invalid max attempts")
	}

	result, err := handler.service.Exec(ctx, &core.JobEnqueueRequest{
		Kind:           request.GetKind(),
		Payload:        request.GetPayload(),
		OwnerID:        ownerID,
		IdempotencyKey: request.IdempotencyKey,
		// Bounds-checked against the smallint range immediately above, so the narrowing cannot overflow.
		MaxAttempts: int16(request.GetMaxAttempts()), //nolint:gosec
	})
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, mapJobError(err)
	}

	return &protogen.JobEnqueueResponse{Job: jobToProto(result.Job), Created: result.Created}, nil
}
