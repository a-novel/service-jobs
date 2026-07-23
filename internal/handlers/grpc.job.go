package handlers

import (
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/a-novel/service-jobs/internal/core"
	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// jobStatusToProto maps a core status onto its protobuf enum. An unknown value maps to UNSPECIFIED
// rather than guessing, so a status the wire does not know about is visibly absent.
var jobStatusToProto = map[core.JobStatus]protogen.JobStatus{
	core.JobStatusPending:   protogen.JobStatus_JOB_STATUS_PENDING,
	core.JobStatusClaimed:   protogen.JobStatus_JOB_STATUS_CLAIMED,
	core.JobStatusSucceeded: protogen.JobStatus_JOB_STATUS_SUCCEEDED,
	core.JobStatusFailed:    protogen.JobStatus_JOB_STATUS_FAILED,
	core.JobStatusAbandoned: protogen.JobStatus_JOB_STATUS_ABANDONED,
	core.JobStatusCancelled: protogen.JobStatus_JOB_STATUS_CANCELLED,
}

// jobToProto converts a core job into its protobuf form. Timestamps are RFC 3339 strings, and an
// unset optional timestamp is the empty string rather than a zero-time that reads as a real date.
func jobToProto(job *core.Job) *protogen.Job {
	return &protogen.Job{
		Id:             job.ID.String(),
		Kind:           job.Kind,
		Payload:        job.Payload,
		OwnerId:        job.OwnerID.String(),
		Status:         jobStatusToProto[job.Status],
		Attempt:        int32(job.Attempt),
		MaxAttempts:    int32(job.MaxAttempts),
		Result:         job.Result,
		Error:          job.Error,
		ProviderCallId: derefString(job.ProviderCallID),
		CreatedAt:      job.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      job.UpdatedAt.Format(time.RFC3339),
		SettledAt:      formatTime(job.SettledAt),
		ExpiresAt:      formatTime(job.ExpiresAt),
	}
}

// mapJobError translates a core error onto a gRPC status. The mapping is the queue's public contract:
// a cross-owner read is not-found rather than permission-denied, so the response is not an existence
// oracle over a priced resource, and a reused idempotency key is already-exists. An unrecognised
// error is internal, with no detail on the wire.
func mapJobError(err error) error {
	switch {
	case errors.Is(err, core.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, "invalid request")
	case errors.Is(err, core.ErrJobNotFound):
		return status.Error(codes.NotFound, "job not found")
	case errors.Is(err, core.ErrJobConflict):
		return status.Error(codes.AlreadyExists, "idempotency key reused for a different request")
	case errors.Is(err, core.ErrJobNotClaimed):
		return status.Error(codes.FailedPrecondition, "job not claimed by this worker")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// derefString returns the empty string for a nil pointer.
func derefString(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

// formatTime renders a timestamp as RFC 3339, or the empty string when it is unset.
func formatTime(value *time.Time) string {
	if value == nil {
		return ""
	}

	return value.Format(time.RFC3339)
}
