package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"go.opentelemetry.io/otel/trace/noop"
)

var testTracer = noop.NewTracerProvider().Tracer("test")

type MockStore struct {
	jobs map[string]*scheduler.Job
}

func NewMockStore() *MockStore {
	return &MockStore{
		jobs: make(map[string]*scheduler.Job),
	}
}

func (m *MockStore) SaveJob(j *scheduler.Job) error {
	m.jobs[j.ID] = j
	return nil
}

func (m *MockStore) SaveJobWithContext(ctx context.Context, j *scheduler.Job) error {
	return m.SaveJob(j)
}

func (m *MockStore) GetJob(id string) (*scheduler.Job, error) {
	return m.jobs[id], nil
}

func (m *MockStore) GetAllJobs() ([]*scheduler.Job, error) {
	var jobs []*scheduler.Job
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (m *MockStore) DeleteJob(id string) error {
	delete(m.jobs, id)
	return nil
}

func (m *MockStore) DeleteJobWithContext(ctx context.Context, id string) error {
	return m.DeleteJob(id)
}

func (m *MockStore) Close() error {
	return nil
}

func TestEngine_AddJob(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	job := &scheduler.Job{
		ID:      "job-1",
		Payload: "test-payload",
		NextRun: time.Now().Add(1 * time.Hour),
	}

	engine.AddJob(job)

	// Verify job is stored
	storedJob, _ := engine.Storage.GetJob("job-1")
	if storedJob == nil {
		t.Error("Job should be saved to storage")
	}
}

func TestEngine_Run(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	// No storage needed for in-memory execution test if we don't care about persistence here
	// But let's add it to avoid nil pointer if code checks it
	engine.Storage = NewMockStore()

	// Add a job that runs very soon
	delay := 100 * time.Millisecond
	job := &scheduler.Job{
		ID:      "job-run-soon",
		Payload: "run-me",
		NextRun: time.Now().Add(delay),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run engine in a goroutine
	go engine.Run(ctx)

	// Wait for job to be processed (in this simple test we just wait a bit)
	time.Sleep(delay + 500*time.Millisecond)

	// In a real test we would mock the execution function or check side effects
	// validation implementation pending better observability in Engine
}

func TestEngine_CustomExecutor(t *testing.T) {
	engine := scheduler.NewEngine(testTracer)
	engine.Storage = NewMockStore()

	executed := make(chan string, 1)
	engine.Executor = func(ctx context.Context, j *scheduler.Job) error {
		executed <- j.ID
		return nil
	}

	job := &scheduler.Job{
		ID:           "job-custom",
		Payload:      "custom-work",
		RateLimitKey: "tenant-1",
		Weight:       2,
		NextRun:      time.Now().Add(50 * time.Millisecond),
	}

	engine.AddJob(job)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go engine.Run(ctx)

	select {
	case id := <-executed:
		if id != "job-custom" {
			t.Fatalf("expected job-custom, got %s", id)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for custom executor to run")
	}
}
