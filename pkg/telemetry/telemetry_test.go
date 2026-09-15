package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

type spanRecorder struct {
	spans []sdktrace.ReadOnlySpan
}

func (s *spanRecorder) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	s.spans = append(s.spans, spans...)
	return nil
}

func (s *spanRecorder) Shutdown(ctx context.Context) error {
	return nil
}

type mockStorage struct {
}
func (m *mockStorage) SaveJob(j *scheduler.Job) error { return nil }
func (m *mockStorage) SaveJobWithContext(ctx context.Context, j *scheduler.Job) error {
	_, span := otel.Tracer("storage").Start(ctx, "SaveJob")
	defer span.End()
	return nil
}
func (m *mockStorage) GetJob(id string) (*scheduler.Job, error) { return nil, nil }
func (m *mockStorage) GetAllJobs() ([]*scheduler.Job, error) { return nil, nil }
func (m *mockStorage) DeleteJob(id string) error { return nil }
func (m *mockStorage) DeleteJobWithContext(ctx context.Context, id string) error { return nil }
func (m *mockStorage) Close() error { return nil }

func TestTelemetrySpans(t *testing.T) {
	// Setup in-memory exporter
	recorder := &spanRecorder{}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(recorder),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("test-service"),
		)),
	)
	otel.SetTracerProvider(tp)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("Error shutting down tracer provider: %v", err)
		}
	}()

	tracer := tp.Tracer("test")

	// Setup Engine and Ring (which have spans)
	engine := scheduler.NewEngine(tracer)
	engine.NodeName = "node-1"
	ring := hash.NewConsistent()
	ring.AddNode("node-1")
	engine.Ring = ring
	engine.Storage = &mockStorage{}

	// Trigger AddJob (contains "AddJob" and "GetNode" spans)
	ctx := context.Background()
	jobID := "test-job-123"
	
	// Start a parent span to see nesting
	ctx, parentSpan := tracer.Start(ctx, "ParentSpan")
	
	// GetNode (hash ring lookup)
	_ = ring.GetNodeWithContext(ctx, jobID)
	
	// AddJob (queue push)
	engine.AddJobWithContext(ctx, &scheduler.Job{
		ID:      jobID,
		NextRun: time.Now().Add(time.Millisecond), // Run almost immediately
	})
	
	// Run engine for one execution
	go engine.Run(ctx)
	
	// Wait for execution to happen
	time.Sleep(500 * time.Millisecond)

	parentSpan.End()

	// Flush spans
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("Error flushing spans: %v", err)
	}

	// Wait a bit for batcher
	time.Sleep(100 * time.Millisecond)

	// Verify spans
	expectedSpans := map[string]bool{
		"GetNode":    false,
		"AddJob":     false,
		"ExecuteJob": false,
		"SaveJob":    false,
		"ParentSpan": false,
	}

	for _, span := range recorder.spans {
		name := span.Name()
		if _, ok := expectedSpans[name]; ok {
			expectedSpans[name] = true
			
			// Verify job_id attribute for relevant spans
			if name == "AddJob" || name == "ExecuteJob" {
				foundID := false
				for _, attr := range span.Attributes() {
					if attr.Key == "job_id" && attr.Value.AsString() == jobID {
						foundID = true
						break
					}
				}
				if !foundID {
					t.Errorf("Span %s missing job_id attribute", name)
				}
			}
		}
	}


	for name, found := range expectedSpans {
		if !found {
			t.Errorf("Expected span %s not found in trace", name)
		}
	}
}
