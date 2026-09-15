package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
)

var recurringTestTracer = noop.NewTracerProvider().Tracer("test")

type mockStorage struct {
	mu   sync.Mutex
	jobs map[string]*Job
}

func (m *mockStorage) SaveJob(j *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}
func (m *mockStorage) SaveJobWithContext(ctx context.Context, j *Job) error { return m.SaveJob(j) }
func (m *mockStorage) GetJob(id string) (*Job, error)                       { return nil, nil }
func (m *mockStorage) GetAllJobs() ([]*Job, error)                         { return nil, nil }
func (m *mockStorage) DeleteJob(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}
func (m *mockStorage) DeleteJobWithContext(ctx context.Context, id string) error { return m.DeleteJob(id) }
func (m *mockStorage) Close() error                                             { return nil }

func TestRecurringJob_MaxRuns(t *testing.T) {
	engine := NewEngine(recurringTestTracer)
	storage := &mockStorage{jobs: make(map[string]*Job)}
	engine.Storage = storage

	// We can observe RunCount in storage or the engine's behavior.
	
	job := &Job{
		ID:             "test-max-runs",
		Payload:        "payload",
		NextRun:        time.Now(),
		RepeatInterval: 100 * time.Millisecond,
		MaxRuns:        3,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	engine.AddJob(job)
	go engine.Run(ctx)

	// Wait for executions
	time.Sleep(1 * time.Second)

	engine.mu.Lock()
	defer engine.mu.Unlock()

	// If MaxRuns=3, it should execute 3 times and then NOT be in the queue.
	if engine.queue.Len() != 0 {
		t.Errorf("Expected queue to be empty after max runs, got %d", engine.queue.Len())
	}

	// Check if it's deleted from storage
	storage.mu.Lock()
	if _, ok := storage.jobs[job.ID]; ok {
		t.Errorf("Expected job to be deleted from storage after max runs")
	}
	storage.mu.Unlock()
	
	if job.RunCount != 3 {
		t.Errorf("Expected 3 runs, got %d", job.RunCount)
	}
}

func TestRecurringJob_Unlimited(t *testing.T) {
	engine := NewEngine(recurringTestTracer)
	storage := &mockStorage{jobs: make(map[string]*Job)}
	engine.Storage = storage

	job := &Job{
		ID:             "test-unlimited",
		Payload:        "payload",
		NextRun:        time.Now(),
		RepeatInterval: 100 * time.Millisecond,
		MaxRuns:        0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine.AddJob(job)
	go engine.Run(ctx)

	// Wait for at least 3 executions. Each takes ~100ms.
	time.Sleep(1 * time.Second)

	if job.RunCount < 2 {
		t.Errorf("Expected at least 2 runs, got %d", job.RunCount)
	}

	// It should either be in the queue or currently executing.
	// Since we waited 1s and it repeats every 100ms, it should definitely have re-enqueued.
	// We'll retry a few times if queue is empty (it might be in execute).
	success := false
	for i := 0; i < 5; i++ {
		engine.mu.Lock()
		if engine.queue.Len() == 1 {
			success = true
			engine.mu.Unlock()
			break
		}
		engine.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	if !success {
		t.Errorf("Expected 1 job in queue (re-enqueued)")
	}
}

func TestOneShotJob(t *testing.T) {
	engine := NewEngine(recurringTestTracer)
	storage := &mockStorage{jobs: make(map[string]*Job)}
	engine.Storage = storage

	job := &Job{
		ID:             "test-one-shot",
		Payload:        "payload",
		NextRun:        time.Now(),
		RepeatInterval: 0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	defer cancel()

	engine.AddJob(job)
	go engine.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	if job.RunCount != 1 {
		t.Errorf("Expected 1 run, got %d", job.RunCount)
	}

	engine.mu.Lock()
	if engine.queue.Len() != 0 {
		t.Errorf("Expected empty queue for one-shot job, got %d", engine.queue.Len())
	}
	engine.mu.Unlock()

	storage.mu.Lock()
	if _, ok := storage.jobs[job.ID]; ok {
		t.Errorf("Expected job to be deleted from storage after one shot")
	}
	storage.mu.Unlock()
}

func TestRecurringJob_NextRunUpdate(t *testing.T) {
	engine := NewEngine(recurringTestTracer)
	
	interval := 500 * time.Millisecond
	job := &Job{
		ID:             "test-next-run",
		Payload:        "payload",
		NextRun:        time.Now(),
		RepeatInterval: interval,
		MaxRuns:        2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine.AddJob(job)
	go engine.Run(ctx)

	// Wait for first execution (~100ms) and re-enqueue.
	// We'll poll for the job to have been executed at least once.
	var nextRun time.Time
	success := false
	for i := 0; i < 20; i++ {
		engine.mu.Lock()
		if engine.queue.Len() == 1 && engine.queue[0].RunCount > 0 {
			nextRun = engine.queue[0].NextRun
			success = true
			engine.mu.Unlock()
			break
		}
		engine.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatal("Job was not re-enqueued")
	}

	expectedMin := time.Now().Add(interval - 500*time.Millisecond) // buffer for execution time
	if nextRun.Before(expectedMin) {
		t.Errorf("NextRun was not updated correctly. Got %v, expected after %v", nextRun, expectedMin)
	}
}

func TestConcurrentRecurringJobs(t *testing.T) {
	engine := NewEngine(recurringTestTracer)
	storage := &mockStorage{jobs: make(map[string]*Job)}
	engine.Storage = storage

	numJobs := 5
	for i := 0; i < numJobs; i++ {
		engine.AddJob(&Job{
			ID:             "job-" + string(rune(i)),
			Payload:        "payload",
			NextRun:        time.Now(),
			RepeatInterval: 50 * time.Millisecond,
			MaxRuns:        2,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	defer cancel()

	go engine.Run(ctx)

	time.Sleep(500 * time.Millisecond)

	engine.mu.Lock()
	if engine.queue.Len() != 0 {
		t.Errorf("Expected all concurrent jobs to finish, but %d remain", engine.queue.Len())
	}
	engine.mu.Unlock()
}
