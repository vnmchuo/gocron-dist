package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"go.opentelemetry.io/otel/trace/noop"
)

var rebalanceTestTracer = noop.NewTracerProvider().Tracer("test")

type mockForwarder struct {
	mu           sync.Mutex
	forwardedJobs []*Job
}

func (f *mockForwarder) ForwardJob(ctx context.Context, j *Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwardedJobs = append(f.forwardedJobs, j)
	return nil
}

func TestRebalance_ForwardsMisownedJobs(t *testing.T) {
	// Setup
	engine := NewEngine(rebalanceTestTracer)
	engine.NodeName = "node-1"
	ring := hash.NewConsistent()
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	engine.Ring = ring

	// Find a job ID that is owned by node-2
	var jobID string
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("job-%d", i)
		if ring.GetNode(id) == "node-2" {
			jobID = id
			break
		}
	}
	if jobID == "" {
		t.Fatal("Could not find a job ID owned by node-2")
	}

	// Add it to node-1's engine
	job := &Job{
		ID:      jobID,
		Payload: "payload",
		NextRun: time.Now().Add(10 * time.Minute),
	}
	engine.AddJob(job)

	// Verify it's in the queue
	engine.mu.Lock()
	if engine.queue.Len() != 1 {
		t.Errorf("Expected 1 job in queue, got %d", engine.queue.Len())
	}
	engine.mu.Unlock()

	// Call Rebalance with a mock forwarder
	f := &mockForwarder{}
	engine.Rebalance(context.Background(), f)

	// Wait for goroutine
	time.Sleep(200 * time.Millisecond)

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.forwardedJobs) != 1 {
		t.Errorf("Expected 1 job to be forwarded, got %d", len(f.forwardedJobs))
	} else if f.forwardedJobs[0].ID != jobID {
		t.Errorf("Expected job %s to be forwarded, got %s", jobID, f.forwardedJobs[0].ID)
	}

	// Verify it's removed from queue (wait for rebalance loop to finish)
	engine.mu.Lock()
	if engine.queue.Len() != 0 {
		t.Errorf("Expected queue to be empty after rebalance, got %d", engine.queue.Len())
	}
	engine.mu.Unlock()
}

func TestRebalance_OnNodeLeave_Simulated(t *testing.T) {
	// This test simulates what happens when a node leaves.
	// 3 Nodes: Node-1, Node-2, Node-3.
	// Node-1 has a job owned by Node-2.
	// Node-2 leaves.
	// Now the job might be owned by Node-3 (or Node-1).
	
	engine1 := NewEngine(rebalanceTestTracer)
	engine1.NodeName = "node-1"
	ring := hash.NewConsistent()
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")
	engine1.Ring = ring

	// Find a job owned by node-2
	var jobID string
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("task-%d", i)
		if ring.GetNode(id) == "node-2" {
			jobID = id
			// Check if after removing node-2, it belongs to node-3
			ring.RemoveNode("node-2")
			ownerAfter := ring.GetNode(id)
			ring.AddNode("node-2") // put it back for now
			if ownerAfter == "node-3" {
				jobID = id
				break
			}
		}
	}
	
	if jobID == "" {
		t.Fatal("Could not find a suitable job ID for leave simulation")
	}

	// Add job to node-1's engine
	engine1.AddJob(&Job{ID: jobID, NextRun: time.Now().Add(time.Hour)})

	// Simulate Node-2 leaving
	ring.RemoveNode("node-2")
	
	f := &mockForwarder{}
	engine1.Rebalance(context.Background(), f)
	
	time.Sleep(100 * time.Millisecond)
	
	f.mu.Lock()
	if len(f.forwardedJobs) != 1 {
		t.Errorf("Expected job to be forwarded to new owner node-3")
	} else {
		newOwner := ring.GetNode(jobID)
		if newOwner != "node-3" && newOwner != "node-1" {
			t.Errorf("Expected new owner to be node-3 or node-1, got %s", newOwner)
		}
	}
	f.mu.Unlock()
}

