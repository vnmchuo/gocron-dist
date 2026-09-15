package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/cluster"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"github.com/vnmchuo/gocron-dist/pkg/api"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
)

// This example demonstrates how to set up a minimal distributed node.
// For a full cluster, you would run multiple instances of this with different -name and -port.
func main() {
	nodeName := "example-node"
	grpcPort := 50060
	gossipPort := 7950

	fmt.Printf("Starting Distributed Node: %s\n", nodeName)

	tracer := noop.NewTracerProvider().Tracer("noop")

	// 1. Ring & Engine
	ring := hash.NewConsistent()
	ring.AddNode(nodeName)
	engine := scheduler.NewEngine(tracer)

	// 2. Cluster Setup
	c, err := cluster.NewCluster(nodeName, gossipPort, grpcPort, "",
		func(name string) { ring.AddNode(name) },
		func(name string) { /* handle leave if needed */ },
	)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}
	_ = c

	// 3. gRPC Server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	api.RegisterSchedulerServiceServer(grpcServer, &api.Server{
		Engine:   engine,
		Ring:     ring,
		NodeName: nodeName,
		Cluster:  c,
		Tracer:   tracer,
	})

	go grpcServer.Serve(lis)
	fmt.Printf("gRPC API ready on :%d\n", grpcPort)

	// 4. Run Scheduler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.Run(ctx)

	// 5. Add a job that this node owns
	engine.AddJob(&scheduler.Job{
		ID:      "distributed-job-1",
		Payload: "Example distributed task",
		NextRun: time.Now().Add(3 * time.Second),
	})

	time.Sleep(10 * time.Second)
	fmt.Println("Distributed example node shutting down.")
}
