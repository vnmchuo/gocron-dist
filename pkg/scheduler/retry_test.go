package scheduler_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
)

func TestEngine_RetrySuccess(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	var attempts atomic.Int32
	engine.Executor = func(ctx context.Context, j *scheduler.Job) error {
		attempt := attempts.Add(1)
		if attempt < 3 {
			return errors.New("transient database connection failure")
		}
		return nil // Success on 3rd attempt
	}

	job := &scheduler.Job{
		ID:             "job-retry-success",
		Payload:        "payload-retry",
		MaxRetries:     3,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond, // Constant 20ms for fast tests
		NextRun:        time.Now().Add(10 * time.Millisecond),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go engine.Run(ctx)

	// Wait for retries to complete
	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for job to succeed after retries, attempts=%d", attempts.Load())
		default:
			if attempts.Load() == 3 {
				time.Sleep(50 * time.Millisecond)
				// Verify job was deleted from storage upon final success
				stored, _ := engine.Storage.GetJob("job-retry-success")
				if stored != nil {
					t.Errorf("job should be deleted from storage after success")
				}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestEngine_RetryExhausted_DLQ(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	var attempts atomic.Int32
	engine.Executor = func(ctx context.Context, j *scheduler.Job) error {
		attempts.Add(1)
		return errors.New("permanent downstream error")
	}

	dlqTriggered := make(chan string, 1)
	engine.DLQHandler = func(ctx context.Context, j *scheduler.Job, finalErr error) {
		dlqTriggered <- j.ID
	}

	job := &scheduler.Job{
		ID:             "job-retry-dlq",
		Payload:        "payload-dlq",
		MaxRetries:     2,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		NextRun:        time.Now().Add(10 * time.Millisecond),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go engine.Run(ctx)

	select {
	case id := <-dlqTriggered:
		if id != "job-retry-dlq" {
			t.Fatalf("expected job-retry-dlq, got %s", id)
		}
		// Initial execution (1) + 2 retries = 3 total attempts
		if count := attempts.Load(); count != 3 {
			t.Fatalf("expected 3 attempts (1 initial + 2 retries), got %d", count)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for DLQHandler to be triggered")
	}
}

func TestEngine_NonRetryableError(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	var attempts atomic.Int32
	engine.Executor = func(ctx context.Context, j *scheduler.Job) error {
		attempts.Add(1)
		return scheduler.ErrDoNotRetry
	}

	dlqTriggered := make(chan string, 1)
	engine.DLQHandler = func(ctx context.Context, j *scheduler.Job, finalErr error) {
		dlqTriggered <- j.ID
	}

	job := &scheduler.Job{
		ID:             "job-non-retryable",
		Payload:        "do-not-retry",
		MaxRetries:     5, // Configured with 5 retries, but should exit on attempt 1!
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		NextRun:        time.Now().Add(10 * time.Millisecond),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go engine.Run(ctx)

	select {
	case id := <-dlqTriggered:
		if id != "job-non-retryable" {
			t.Fatalf("expected job-non-retryable, got %s", id)
		}
		if count := attempts.Load(); count != 1 {
			t.Fatalf("expected exactly 1 attempt for non-retryable error, got %d", count)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for DLQHandler")
	}
}

func TestEngine_PanicRecoveryAndRetry(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	var attempts atomic.Int32
	engine.Executor = func(ctx context.Context, j *scheduler.Job) error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			panic("unexpected nil pointer dereference inside user job!")
		}
		return nil // Succeeded on attempt 2
	}

	job := &scheduler.Job{
		ID:             "job-panic-retry",
		Payload:        "panic-work",
		MaxRetries:     2,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		NextRun:        time.Now().Add(10 * time.Millisecond),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go engine.Run(ctx)

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for panic recovery retry to finish, attempts=%d", attempts.Load())
		default:
			if attempts.Load() == 2 {
				return // Succeeded!
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
