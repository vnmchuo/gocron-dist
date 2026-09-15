package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	go_trace "go.opentelemetry.io/otel/trace"
)

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
	))
	defer span.End()

	// Execution must be safe from panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Recover] Job %s failed: %v\n", j.ID, r)
			span.RecordError(fmt.Errorf("panic: %v", r))
		}
	}()

	if e.Executor != nil {
		if err := e.Executor(ctx, j); err != nil {
			log.Printf("[Engine] Job %s execution failed or deferred: %v\n", j.ID, err)
			span.RecordError(err)
			return
		}
	} else {
		log.Printf("[%s] Executing job: %s\n", time.Now().Format("15:04:05"), j.Payload)
		// Simulate work
		time.Sleep(100 * time.Millisecond)
	}

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
		log.Printf("[Scheduler] Re-scheduling job %s for %s\n", j.ID, j.NextRun.Format("15:04:05"))
		e.AddJobWithContext(ctx, j)
	} else {
		// Delete job from storage to prevent re-execution on restart
		if e.Storage != nil {
			if err := e.Storage.DeleteJobWithContext(ctx, j.ID); err != nil {
				log.Printf("[Error] Failed to delete job %s: %v\n", j.ID, err)
			} else {
				log.Printf("[Storage] Job %s deleted from disk\n", j.ID)
			}
		}
	}
}
