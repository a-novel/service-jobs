package servicejobs

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	golibproto "github.com/a-novel-kit/golib/grpcf/proto/gen"

	"github.com/a-novel/service-jobs/internal/handlers/protogen"
)

// Request, response, and entity types are re-exported from the service's generated protobuf
// definitions, so callers never import the service's internal packages.
type (
	StatusRequest  = protogen.StatusRequest
	StatusResponse = protogen.StatusResponse
	// QueueDepth is the backlog carried on StatusResponse.Queue: the pending count and the
	// oldest-pending age.
	QueueDepth = protogen.QueueDepth

	JobEnqueueRequest  = protogen.JobEnqueueRequest
	JobEnqueueResponse = protogen.JobEnqueueResponse
	JobGetRequest      = protogen.JobGetRequest
	JobGetResponse     = protogen.JobGetResponse
	JobClaimRequest    = protogen.JobClaimRequest
	JobClaimResponse   = protogen.JobClaimResponse
	JobSettleRequest   = protogen.JobSettleRequest
	JobSettleResponse  = protogen.JobSettleResponse
	JobWatchRequest    = protogen.JobWatchRequest
	JobWatchResponse   = protogen.JobWatchResponse

	Job        = protogen.Job
	JobStatus  = protogen.JobStatus
	JobFailure = protogen.JobFailure

	// JobSettleResult and JobSettleFailure are the two arms of the JobSettle outcome oneof: a caller
	// sets one on JobSettleRequest.Outcome.
	JobSettleResult  = protogen.JobSettleRequest_Result
	JobSettleFailure = protogen.JobSettleRequest_Failure

	// JobWatchClient streams a watched job's snapshots. Receive from it until io.EOF, which the
	// server sends once the job is terminal.
	JobWatchClient = grpc.ServerStreamingClient[protogen.JobWatchResponse]
)

// JobStatus values re-exported so a caller can branch on a job's lifecycle without importing the
// generated package.
const (
	JobStatusUnspecified = protogen.JobStatus_JOB_STATUS_UNSPECIFIED
	JobStatusPending     = protogen.JobStatus_JOB_STATUS_PENDING
	JobStatusClaimed     = protogen.JobStatus_JOB_STATUS_CLAIMED
	JobStatusSucceeded   = protogen.JobStatus_JOB_STATUS_SUCCEEDED
	JobStatusFailed      = protogen.JobStatus_JOB_STATUS_FAILED
	JobStatusAbandoned   = protogen.JobStatus_JOB_STATUS_ABANDONED
	JobStatusCancelled   = protogen.JobStatus_JOB_STATUS_CANCELLED
)

// A Client issues the service's gRPC calls, one method per RPC. Construct one with [NewClient] and
// call Close when finished to release the connection.
type Client interface {
	UnaryEcho(
		ctx context.Context, req *golibproto.UnaryEchoRequest, opts ...grpc.CallOption,
	) (*golibproto.UnaryEchoResponse, error)
	Status(ctx context.Context, req *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)

	JobEnqueue(ctx context.Context, req *JobEnqueueRequest, opts ...grpc.CallOption) (*JobEnqueueResponse, error)
	JobGet(ctx context.Context, req *JobGetRequest, opts ...grpc.CallOption) (*JobGetResponse, error)
	JobClaim(ctx context.Context, req *JobClaimRequest, opts ...grpc.CallOption) (*JobClaimResponse, error)
	JobSettle(ctx context.Context, req *JobSettleRequest, opts ...grpc.CallOption) (*JobSettleResponse, error)
	JobWatch(ctx context.Context, req *JobWatchRequest, opts ...grpc.CallOption) (JobWatchClient, error)

	// Close releases the underlying gRPC connection. Call it once the client is no longer needed.
	Close()
}

type client struct {
	golibproto.EchoServiceClient
	protogen.StatusServiceClient
	protogen.JobEnqueueServiceClient
	protogen.JobGetServiceClient
	protogen.JobClaimServiceClient
	protogen.JobSettleServiceClient
	protogen.JobWatchServiceClient

	conn *grpc.ClientConn
}

func (c *client) Close() {
	_ = c.conn.Close()
}

// NewClient creates a [Client] for the service reachable at addr. The connection is established
// lazily on the first RPC. Dial options are forwarded to the underlying gRPC connection.
func NewClient(addr string, opts ...grpc.DialOption) (Client, error) {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("new grpc client: %w", err)
	}

	return &client{
		EchoServiceClient:       golibproto.NewEchoServiceClient(conn),
		StatusServiceClient:     protogen.NewStatusServiceClient(conn),
		JobEnqueueServiceClient: protogen.NewJobEnqueueServiceClient(conn),
		JobGetServiceClient:     protogen.NewJobGetServiceClient(conn),
		JobClaimServiceClient:   protogen.NewJobClaimServiceClient(conn),
		JobSettleServiceClient:  protogen.NewJobSettleServiceClient(conn),
		JobWatchServiceClient:   protogen.NewJobWatchServiceClient(conn),
		conn:                    conn,
	}, nil
}
