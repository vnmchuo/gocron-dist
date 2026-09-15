package api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/cluster"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	UnimplementedSchedulerServiceServer
	Engine   *scheduler.Engine
	Ring     *hash.Consistent
	Cluster  *cluster.Cluster
	NodeName string
	Tracer   trace.Tracer
}

func (s *Server) AddJob(ctx context.Context, req *AddJobRequest) (*AddJobResponse, error) {
	ctx, span := s.Tracer.Start(ctx, "AddJob", trace.WithAttributes(
		attribute.String("job_id", req.Id),
		attribute.String("node_name", s.NodeName),
	))
	defer span.End()

	// 1. Determine who owns the job based on its ID
	owner := s.Ring.GetNodeWithContext(ctx, req.Id)

	// 2. If the owner is not this node, forward the request!
	if owner != s.NodeName && owner != "" {
		// Find the gRPC address of the owner node
		addr, err := s.Cluster.GetNodeGrpcAddress(owner)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve owner %s: %v", owner, err)
		}

		log.Printf("[Forward] Forwarding job %s to %s (%s)\n", req.Id, owner, addr)

		// Dial the owner node
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to owner %s: %v", owner, err)
		}
		defer conn.Close()

		client := NewSchedulerServiceClient(conn)
		
		// Set a timeout for the forwarding call
		fwdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		
		return client.AddJob(fwdCtx, req)
	}

	// 3. If it belongs to this node, add it to the Engine
	s.Engine.AddJob(&scheduler.Job{
		ID:             req.Id,
		Payload:        req.Payload,
		NextRun:        req.ScheduleTime.AsTime(),
		RepeatInterval: time.Duration(req.RepeatIntervalNanos),
		MaxRuns:        int(req.MaxRuns),
	})

	return &AddJobResponse{
		Success:      true,
		Message:      "Job scheduled successfully locally",
		AssignedNode: s.NodeName,
	}, nil
}

func (s *Server) ForwardJob(ctx context.Context, j *scheduler.Job) error {
	req := &AddJobRequest{
		Id:                  j.ID,
		Payload:             j.Payload,
		ScheduleTime:        timestamppb.New(j.NextRun),
		RepeatIntervalNanos: int64(j.RepeatInterval),
		MaxRuns:             int32(j.MaxRuns),
	}
	_, err := s.AddJob(ctx, req)
	return err
}
