// Package grpcserver implements the JobQueue gRPC service on top of
// the storage layer. It is a thin translation layer: proto messages in
// and out, store calls in between. Claim/execute/retry logic stays in
// internal/worker — this package never touches that.
package grpcserver

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/pb/jobqueuepb"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
	"github.com/Harshilagg/Observable_Job_Queue/internal/tracing"
)

var tracer = tracing.Tracer("grpcserver")

// Server implements jobqueuepb.JobQueueServer.
type Server struct {
	jobqueuepb.UnimplementedJobQueueServer
	store *store.Store
}

func New(st *store.Store) *Server {
	return &Server{store: st}
}

// Submit enqueues a new job and returns its id. This is the root of a
// job's trace: its span context is captured and stored on the row
// itself (see internal/tracing's package doc), so the worker that
// eventually claims this job can continue the same trace instead of
// starting a disconnected one.
func (s *Server) Submit(ctx context.Context, req *jobqueuepb.SubmitRequest) (*jobqueuepb.SubmitResponse, error) {
	ctx, span := tracer.Start(ctx, "submit")
	defer span.End()

	id, err := s.store.Enqueue(ctx, req.GetType(), req.GetPayload(), tracing.Inject(ctx))
	if err != nil {
		span.RecordError(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &jobqueuepb.SubmitResponse{Id: id}, nil
}

// GetStatus returns a single snapshot of a job's current state.
func (s *Server) GetStatus(ctx context.Context, req *jobqueuepb.GetStatusRequest) (*jobqueuepb.GetStatusResponse, error) {
	snap, err := s.store.GetStatus(ctx, req.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &jobqueuepb.GetStatusResponse{
		Status: &jobqueuepb.StatusUpdate{
			Id:          snap.ID,
			Status:      string(snap.Status),
			Attempts:    int32(snap.Attempts),
			MaxAttempts: int32(snap.MaxAttempts),
			LastError:   snap.LastError,
		},
	}, nil
}

// WatchStatus polls a job's status and streams updates to the client
// until it reaches a terminal state, then closes the stream. This is
// polling under the hood, not push-based — see the design note in the
// walkthrough for why that's the right call here.
func (s *Server) WatchStatus(req *jobqueuepb.WatchStatusRequest, stream jobqueuepb.JobQueue_WatchStatusServer) error {
	const pollInterval = 500 * time.Millisecond

	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		snap, err := s.store.GetStatus(stream.Context(), req.GetId())
		if errors.Is(err, store.ErrNotFound) {
			return status.Error(codes.NotFound, "job not found")
		}
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		update := &jobqueuepb.StatusUpdate{
			Id:          snap.ID,
			Status:      string(snap.Status),
			Attempts:    int32(snap.Attempts),
			MaxAttempts: int32(snap.MaxAttempts),
			LastError:   snap.LastError,
		}
		if err := stream.Send(update); err != nil {
			return err
		}
		if snap.Status == job.StatusCompleted || snap.Status == job.StatusFailed {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(pollInterval):
		}
	}
}
