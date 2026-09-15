package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	go_trace "go.opentelemetry.io/otel/trace"
)

// ErrDoNotRetry can be returned by a JobExecutor to prevent retrying a failed job.
var ErrDoNotRetry = errors.New("do not retry")

// ErrJobDeferred can be returned by a JobExecutor when a job has been deferred/re-scheduled
// (e.g. due to rate limiting) and should not be deleted from storage or marked as an error.
var ErrJobDeferred = errors.New("job deferred")

type Storer interface {
	SaveJob(j *Job) error
	SaveJobWithContext(ctx context.Context, j *Job) error
	GetJob(id string) (*Job, error)
	GetAllJobs() ([]*Job, error)
	DeleteJob(id string) error
	DeleteJobWithContext(ctx context.Context, id string) error
	Close() error
}

// JobExecutor is a function executed when a scheduled job is due.
// If it returns an error, the error is recorded and job lifecycle is handled accordingly.
type JobExecutor func(ctx context.Context, j *Job) error

// DLQHandler is an optional callback invoked when a job exhausts all retry attempts.
type DLQHandler func(ctx context.Context, j *Job, finalErr error)

type Engine struct {
	queue      JobQueue
	mu         sync.Mutex
	newJobChan chan struct{}
	Storage    Storer
	NodeName   string
	Ring       Router
	Cluster    Manager
	Tracer     trace.Tracer
	Executor   JobExecutor // Pluggable job execution callback
	DLQHandler DLQHandler  // Dead Letter Queue callback on exhausted retries
}

type Router interface {
	GetNode(key string) string
	GetNodeWithContext(ctx context.Context, key string) string
}

type Manager interface {
	GetNodeGrpcAddress(nodeName string) (string, error)
}

type Forwarder interface {
	ForwardJob(ctx context.Context, j *Job) error
}

func NewEngine(t trace.Tracer) *Engine {
	e := &Engine{
		queue:      make(JobQueue, 0),
		newJobChan: make(chan struct{}, 1),
		Tracer:     t,
	}
	heap.Init(&e.queue)
	return e
}

// Rebalance checks if this node still owns its jobs. If not, it forwards them.
// Note: This implementation only rebalances jobs that are ALREADY present in the
// memory/storage of the surviving nodes. If a node fails abruptly, any jobs
// stored EXCLUSIVELY in its local PebbleDB are lost until that node or its
// storage becomes accessible again. Re-assignment is best-effort.
func (e *Engine) Rebalance(ctx context.Context, f Forwarder) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.Ring == nil {
		return
	}

	var remainingJobs []*Job
	for e.queue.Len() > 0 {
		j := heap.Pop(&e.queue).(*Job)
		owner := e.Ring.GetNode(j.ID)

		if owner != e.NodeName && owner != "" && f != nil {
			log.Printf("[Rebalance] Forwarding job %s to new owner %s\n", j.ID, owner)
			go func(job *Job) {
				if err := f.ForwardJob(ctx, job); err != nil {
					log.Printf("[Error] Failed to forward rebalanced job %s: %v. Re-queuing locally.\n", job.ID, err)
					// Fallback: re-queue locally to prevent job loss on forward failure
					e.AddJobWithContext(ctx, job)
				} else {
					// Delete from local storage after successful forward
					if e.Storage != nil {
						_ = e.Storage.DeleteJob(job.ID)
					}
				}
			}(j)
		} else {
			remainingJobs = append(remainingJobs, j)
		}
	}

	for _, j := range remainingJobs {
		heap.Push(&e.queue, j)
	}
}

// AddJob adds a new job to the heap and triggers a timer re-evaluation
func (e *Engine) AddJob(j *Job) {
	e.AddJobWithContext(context.Background(), j)
}

func (e *Engine) AddJobWithContext(ctx context.Context, j *Job) {
	ctx, span := e.Tracer.Start(ctx, "AddJob", go_trace.WithAttributes(
		attribute.String("job_id", j.ID),
		attribute.String("next_run", j.NextRun.String()),
	))
	defer span.End()

	e.mu.Lock()

	if e.Storage != nil {
		if err := e.Storage.SaveJobWithContext(ctx, j); err != nil {
			log.Printf("[Error] Failed to save job %s at runtime: %v\n", j.ID, err)
		}
	}

	heap.Push(&e.queue, j)
	e.mu.Unlock()

	// Notify engine of new job (NextRun might be earlier than currently waited)
	select {
	case e.newJobChan <- struct{}{}:
	default:
	}
}

func (e *Engine) Run(ctx context.Context) {
	log.Println("Scheduler Engine started...")

	for {
		e.mu.Lock()
		var nextRun time.Duration = 1 * time.Hour // Default wait if queue is empty

		if e.queue.Len() > 0 {
			now := time.Now()
			nextJob := e.queue[0]

			if now.After(nextJob.NextRun) || now.Equal(nextJob.NextRun) {
				// Time to execute!
				job := heap.Pop(&e.queue).(*Job)
				e.mu.Unlock()

				go e.execute(job)
				continue
			}
			nextRun = time.Until(nextJob.NextRun)
		}
		e.mu.Unlock()

		timer := time.NewTimer(nextRun)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			// Timer expired, loop again to execute top job
		case <-e.newJobChan:
			// New job received, reset timer to check if this new job is more urgent
			timer.Stop()
		}
	}
}

func (e *Engine) execute(j *Job) {
	ctx, span := e.Tracer.Start(context.Background(), "ExecuteJob", go_trace.WithAttributes(
		attribute.String("job_id", j.ID),
		attribute.String("payload", j.Payload),
		attribute.Int("run_count", j.RunCount),
		attribute.Int("retry_count", j.RetryCount),
	))
	defer span.End()

	var execErr error

	// Execution must be safe from panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Recover] Job %s panicked: %v\n", j.ID, r)
			panicErr := fmt.Errorf("panic: %v", r)
			span.RecordError(panicErr)
			e.handleFailure(ctx, j, panicErr)
		}
	}()

	if e.Executor != nil {
		if err := e.Executor(ctx, j); err != nil {
			execErr = err
		}
	} else {
		log.Printf("[%s] Executing job: %s\n", time.Now().Format("15:04:05"), j.Payload)
		// Simulate work
		time.Sleep(100 * time.Millisecond)
	}

	if execErr != nil {
		if errors.Is(execErr, ErrJobDeferred) {
			log.Printf("[Engine] Job %s deferred and re-queued by executor", j.ID)
			return
		}
		log.Printf("[Engine] Job %s execution failed: %v\n", j.ID, execErr)
		span.RecordError(execErr)
		e.handleFailure(ctx, j, execErr)
		return
	}

	// Succeeded: reset retry counter
	j.RetryCount = 0
	j.RunCount++

	// Check if job should be repeated
	shouldRepeat := false
	if j.RepeatInterval > 0 {
		if j.MaxRuns <= 0 || j.RunCount < j.MaxRuns {
			shouldRepeat = true
		}
	}

	if shouldRepeat {
		j.NextRun = time.Now().Add(j.RepeatInterval)
		log.Printf("[Scheduler] Re-scheduling recurring job %s for %s\n", j.ID, j.NextRun.Format("15:04:05"))
		e.AddJobWithContext(ctx, j)
	} else {
		// Delete job from storage to prevent re-execution on restart
		if e.Storage != nil {
			if err := e.Storage.DeleteJobWithContext(ctx, j.ID); err != nil {
				log.Printf("[Error] Failed to delete job %s: %v\n", j.ID, err)
			} else {
				log.Printf("[Storage] Job %s completed and deleted from disk\n", j.ID)
			}
		}
	}
}

func (e *Engine) handleFailure(ctx context.Context, j *Job, err error) {
	if errors.Is(err, ErrDoNotRetry) {
		log.Printf("[Engine] Job %s marked as non-retryable", j.ID)
		e.terminalFailure(ctx, j, err)
		return
	}

	if j.RetryCount < j.MaxRetries {
		j.RetryCount++
		delay := e.calculateBackoff(j)
		j.NextRun = time.Now().Add(delay)
		log.Printf("[Retry 🔄] Job %s failed (attempt %d/%d): %v. Retrying in %v (scheduled for %s)",
			j.ID, j.RetryCount, j.MaxRetries, err, delay, j.NextRun.Format("15:04:05.000"))
		e.AddJobWithContext(ctx, j)
		return
	}

	// Max retries reached
	e.terminalFailure(ctx, j, err)
}

func (e *Engine) calculateBackoff(j *Job) time.Duration {
	base := j.InitialBackoff
	if base <= 0 {
		base = 1 * time.Second
	}
	maxB := j.MaxBackoff
	if maxB <= 0 {
		maxB = 1 * time.Minute
	}

	if base >= maxB {
		return maxB
	}

	multiplier := 1
	if j.RetryCount > 1 {
		shift := j.RetryCount - 1
		if shift > 30 {
			shift = 30
		}
		multiplier = 1 << uint(shift)
	}

	backoff := time.Duration(multiplier) * base
	if backoff > maxB || backoff <= 0 {
		backoff = maxB
	}

	// Add Full Jitter: randomize between [base/2, backoff]
	if backoff > base {
		jitterRange := int64(backoff - base/2)
		if jitterRange > 0 {
			backoff = base/2 + time.Duration(rand.Int63n(jitterRange))
		}
	}

	return backoff
}

func (e *Engine) terminalFailure(ctx context.Context, j *Job, err error) {
	log.Printf("[DLQ / Failed ❌] Job %s exhausted all %d retries. Final error: %v",
		j.ID, j.RetryCount, err)

	if e.DLQHandler != nil {
		e.DLQHandler(ctx, j, err)
	}

	if e.Storage != nil {
		if delErr := e.Storage.DeleteJobWithContext(ctx, j.ID); delErr != nil {
			log.Printf("[Error] Failed to delete failed job %s from storage: %v", j.ID, delErr)
		}
	}
}
