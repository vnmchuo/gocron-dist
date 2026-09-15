package main

import (
	"context"
	"fmt"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"go.opentelemetry.io/otel/trace/noop"
)

func main() {
	// 1. Initialize the Scheduler Engine with a no-op tracer
	engine := scheduler.NewEngine(noop.NewTracerProvider().Tracer("noop"))

	// 2. Start the Engine in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go engine.Run(ctx)
	fmt.Println("Basic Scheduler Engine started...")

	// 3. Add some jobs
	fmt.Println("Scheduling job-1 (5s delay)...")
	engine.AddJob(&scheduler.Job{
		ID:      "job-1",
		Payload: "Task 1: Say hello",
		NextRun: time.Now().Add(5 * time.Second),
	})

	fmt.Println("Scheduling job-2 (2s delay)...")
	engine.AddJob(&scheduler.Job{
		ID:      "job-2",
		Payload: "Task 2: Cleanup work",
		NextRun: time.Now().Add(2 * time.Second),
	})

	// 4. Wait to see execution
	fmt.Println("Waiting for jobs to execute (max 10s)...")
	time.Sleep(10 * time.Second)
	fmt.Println("Basic usage example finished.")
}
