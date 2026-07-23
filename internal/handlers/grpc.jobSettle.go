package handlers

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// JobSettleService records the outcome of a run. The core layer supplies the implementation, which
// folds a retryable failure into a requeue-or-give-up decision.
type JobSettleService interface {
	Exec(ctx context.Context, request *core.JobSettleRequest) (*core.Job, error)
}

// JobSettle is the gRPC handler for the JobSettle RPC.
type JobSettle struct {
	protogen.UnimplementedJobSettleServiceServer

	service JobSettleService
}

// NewJobSettle returns a new JobSettle handler.
func NewJobSettle(service JobSettleService) *JobSettle {
	return &JobSettle{service: service}
}

func (handler *JobSettle) JobSettle(
	ctx context.Context, request *protogen.JobSettleRequest,
) (*protogen.JobSettleResponse, error) {
	ctx, span := otel.Tracer().Start(ctx, "grpc.JobSettle")
	defer span.End()

	id, err := uuid.Parse(request.GetId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, status.Error(codes.InvalidArgument, "invalid job id")
	}

	settleRequest := &core.JobSettleRequest{ID: id, WorkerID: request.GetWorkerId()}

	// The oneof carries the outcome: a failure sets Failure, a success sets the result bytes. A
	// GetFailure of nil is the success arm, whatever the result bytes are.
	if failure := request.GetFailure(); failure != nil {
		settleRequest.Failure = &core.JobFailure{Error: failure.GetError(), Retryable: failure.GetRetryable()}
	} else {
		settleRequest.Result = request.GetResult()
	}

	job, err := handler.service.Exec(ctx, settleRequest)
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, mapJobError(err)
	}

	return &protogen.JobSettleResponse{Job: jobToProto(job)}, nil
}
