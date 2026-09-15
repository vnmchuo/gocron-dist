package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/cluster"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"github.com/vnmchuo/gocron-dist/pkg/storage"
	"github.com/vnmchuo/gocron-dist/pkg/telemetry"
	"github.com/vnmchuo/gocron-dist/pkg/api"
	"google.golang.org/grpc"
)

func main() {

	// 1. Get configuration from flags
	grpcPort := flag.Int("grpc-port", 50051, "Port for gRPC API")
	nodeName := flag.String("name", "", "Unique name for this node")
	port := flag.Int("port", 7946, "Port for Gossip Protocol")
	joinAddr := flag.String("join", "", "Address of another node to join the cluster (optional)")
	flag.Parse()

	if *nodeName == "" {
		log.Fatal("Node name is required! Example: -name=node-1")
	}

	// Initialize Telemetry
	tp, err := telemetry.InitTracer(*nodeName)
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer telemetry.ShutdownTracer(context.Background(), tp)

	tracer := tp.Tracer("gocron-dist")

	// 2. Initialize Consistent Hashing
	ring := hash.NewConsistent()
	ring.AddNode(*nodeName) // Add self to the ring

	dbDir := fmt.Sprintf("data_%s", *nodeName)
	store, _ := storage.NewStore(dbDir, tracer)
	defer store.Close()

	// 3. Initialize Scheduler Engine
	engine := scheduler.NewEngine(tracer)
	engine.Storage = store
	engine.NodeName = *nodeName
	engine.Ring = ring

	// 4. Initialize Cluster (Memberlist)
	// We provide callbacks: if a node Joins/Leaves, update the ring!
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var server *api.Server

	c, err := cluster.NewCluster(
		*nodeName,
		*port,
		*grpcPort,
		*joinAddr,
		func(name string) {
			fmt.Printf("\n[Cluster] Node %s joined.\n", name)
			ring.AddNode(name)
		},
		func(name string) {
			fmt.Printf("\n[Cluster] Node %s left.\n", name)
			ring.RemoveNode(name)
			// Trigger rebalance when someone leaves
			if server != nil {
				go engine.Rebalance(ctx, server)
			}
		},
	)
	if err != nil {
		log.Fatalf("Failed to create cluster: %v", err)
	}
	engine.Cluster = c

	server = &api.Server{
		Engine:   engine,
		Ring:     ring,
		NodeName: *nodeName,
		Cluster:  c,
		Tracer:   tracer,
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	api.RegisterSchedulerServiceServer(grpcServer, server)

	// Start gRPC server
	fmt.Printf("[gRPC] Server running on port %d\n", *grpcPort)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	oldJobs, _ := store.GetAllJobs()
	for _, j := range oldJobs {
		engine.AddJob(j)
		fmt.Printf("[Recovery] Loaded job %s from disk\n", j.ID)
	}

	// 5. Run Engine in Goroutine
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go engine.Run(ctx)

	// 6. Simulation: Add job every 10 seconds
	// Here "Magic" happens: Engine only executes if ring.GetNode(jobID) == nodeName
	go func() {
		counter := 0
		for {
			counter++
			jobID := fmt.Sprintf("job-%d", counter)

			// Check ownership
			owner := ring.GetNode(jobID)
			fmt.Printf("[Check] %s owned by %s\n", jobID, owner)

			if owner == *nodeName {
				engine.AddJob(&scheduler.Job{
					ID:      jobID,
					Payload: fmt.Sprintf("Data for %s", jobID),
					NextRun: time.Now().Add(5 * time.Second),
				})
			}
			time.Sleep(10 * time.Second)
		}
	}()

	// Wait for stop signal (Ctrl+C)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("Shutting down node...")
	cancel() // Cancel context for engine and cluster
	// Wait a moment for any pending operations
	time.Sleep(500 * time.Millisecond)
}
