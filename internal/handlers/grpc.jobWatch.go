package handlers

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel-kit/golib/otel"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// JobWatchService reads one of an owner's jobs. The watch handler polls it and streams each change,
// so it takes the same read the get handler does.
type JobWatchService interface {
	Exec(ctx context.Context, request *core.JobGetRequest) (*core.Job, error)
}

// JobWatch is the gRPC handler for the JobWatch streaming RPC. It sends the job's current state on
// subscribe and one snapshot on each status change, until the job is terminal or the caller
// disconnects.
//
// The stream reads durable state rather than consuming events, which is what makes it resumable: a
// caller that reconnects gets the current state immediately, with no cursor and no missed-change
// window. It learns of a change by polling, since nothing in the stack pushes one; a future
// settle-side notification would only shorten the detection delay, not change the contract.
type JobWatch struct {
	protogen.UnimplementedJobWatchServiceServer

	service      JobWatchService
	pollInterval time.Duration
}

// NewJobWatch returns a new JobWatch handler polling at the given interval.
func NewJobWatch(service JobWatchService, pollInterval time.Duration) *JobWatch {
	return &JobWatch{service: service, pollInterval: pollInterval}
}

func (handler *JobWatch) JobWatch(
	request *protogen.JobWatchRequest, stream protogen.JobWatchService_JobWatchServer,
) error {
	ctx, span := otel.Tracer().Start(stream.Context(), "grpc.JobWatch")
	defer span.End()

	id, err := uuid.Parse(request.GetId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return status.Error(codes.InvalidArgument, "invalid job id")
	}

	ownerID, err := uuid.Parse(request.GetOwnerId())
	if err != nil {
		_ = otel.ReportError(span, err)

		return status.Error(codes.InvalidArgument, "invalid owner id")
	}

	getRequest := &core.JobGetRequest{ID: id, OwnerID: ownerID}

	ticker := time.NewTicker(handler.pollInterval)
	defer ticker.Stop()

	var (
		lastStatus core.JobStatus
		sent       bool
	)

	for {
		job, err := handler.service.Exec(ctx, getRequest)
		if err != nil {
			_ = otel.ReportError(span, err)

			return mapJobError(err)
		}

		// Send on subscribe, then only when the status actually changes, so a caller does not receive
		// one redundant snapshot per poll while the job sits in the same state.
		if !sent || job.Status != lastStatus {
			err = stream.Send(&protogen.JobWatchResponse{Job: jobToProto(job)})
			if err != nil {
				return otel.ReportError(span, err)
			}

			lastStatus = job.Status
			sent = true
		}

		if isTerminalStatus(job.Status) {
			otel.ReportSuccessNoContent(span)

			return nil
		}

		select {
		case <-ctx.Done():
			// A caller that hangs up is not an error: the stream ends where the client left it.
			return status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
}

// isTerminalStatus reports whether a job has reached a settled status, past which it never changes.
func isTerminalStatus(s core.JobStatus) bool {
	switch s {
	case core.JobStatusSucceeded, core.JobStatusFailed, core.JobStatusAbandoned, core.JobStatusCancelled:
		return true
	default:
		return false
	}
}
