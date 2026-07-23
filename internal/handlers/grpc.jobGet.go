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

// JobGetService reads one of an owner's jobs. The core layer supplies the implementation.
type JobGetService interface {
	Exec(ctx context.Context, request *core.JobGetRequest) (*core.Job, error)
}

// JobGet is the gRPC handler for the JobGet RPC.
type JobGet struct {
	protogen.UnimplementedJobGetServiceServer

	service JobGetService
}

// NewJobGet returns a new JobGet handler.
func NewJobGet(service JobGetService) *JobGet {
	return &JobGet{service: service}
}

func (handler *JobGet) JobGet(
	ctx context.Context, request *protogen.JobGetRequest,
) (*protogen.JobGetResponse, error) {
	ctx, span := otel.Tracer().Start(ctx, "grpc.JobGet")
	defer span.End()

	id, err := uuid.Parse(request.GetId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, status.Error(codes.InvalidArgument, "invalid job id")
	}

	ownerID, err := uuid.Parse(request.GetOwnerId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, status.Error(codes.InvalidArgument, "invalid owner id")
	}

	job, err := handler.service.Exec(ctx, &core.JobGetRequest{ID: id, OwnerID: ownerID})
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, mapJobError(err)
	}

	return &protogen.JobGetResponse{Job: jobToProto(job)}, nil
}
