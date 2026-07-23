package handlers

import (
	"context"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// JobClaimService takes a batch of pending jobs for a worker. The core layer supplies the
// implementation.
type JobClaimService interface {
	Exec(ctx context.Context, request *core.JobClaimRequest) ([]*core.Job, error)
}

// JobClaim is the gRPC handler for the JobClaim RPC.
type JobClaim struct {
	protogen.UnimplementedJobClaimServiceServer

	service JobClaimService
}

// NewJobClaim returns a new JobClaim handler.
func NewJobClaim(service JobClaimService) *JobClaim {
	return &JobClaim{service: service}
}

func (handler *JobClaim) JobClaim(
	ctx context.Context, request *protogen.JobClaimRequest,
) (*protogen.JobClaimResponse, error) {
	ctx, span := otel.Tracer().Start(ctx, "grpc.JobClaim")
	defer span.End()

	jobs, err := handler.service.Exec(ctx, &core.JobClaimRequest{
		Kinds:        request.GetKinds(),
		WorkerID:     request.GetWorkerId(),
		Limit:        int(request.GetLimit()),
		LeaseSeconds: int(request.GetLeaseSeconds()),
	})
	if err != nil {
		_ = otel.ReportError(span, err)

		return nil, mapJobError(err)
	}

	protoJobs := make([]*protogen.Job, len(jobs))
	for i, job := range jobs {
		protoJobs[i] = jobToProto(job)
	}

	return &protogen.JobClaimResponse{Jobs: protoJobs}, nil
}
