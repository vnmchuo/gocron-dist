package storage

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/pebble"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	go_trace "go.opentelemetry.io/otel/trace"
)

type Store struct {
	db     *pebble.DB
	Tracer trace.Tracer
}

func NewStore(dir string, t trace.Tracer) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db, Tracer: t}, nil
}

func (s *Store) SaveJob(j *scheduler.Job) error {
	return s.SaveJobWithContext(context.Background(), j)
}

func (s *Store) SaveJobWithContext(ctx context.Context, j *scheduler.Job) error {
	_, span := s.Tracer.Start(ctx, "SaveJob", go_trace.WithAttributes(
		attribute.String("job_id", j.ID),
	))
	defer span.End()

	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return s.db.Set([]byte(j.ID), data, pebble.Sync)
}

func (s *Store) GetAllJobs() ([]*scheduler.Job, error) {
	var jobs []*scheduler.Job

	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var j scheduler.Job
		if err := json.Unmarshal(iter.Value(), &j); err != nil {
			continue // or return err if you want to be strict
		}
		jobs = append(jobs, &j)
	}

	return jobs, nil
}

func (s *Store) GetJob(id string) (*scheduler.Job, error) {
	val, closer, err := s.db.Get([]byte(id))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var j scheduler.Job
	if err := json.Unmarshal(val, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) DeleteJob(id string) error {
	return s.DeleteJobWithContext(context.Background(), id)
}

func (s *Store) DeleteJobWithContext(ctx context.Context, id string) error {
	_, span := s.Tracer.Start(ctx, "DeleteJob", go_trace.WithAttributes(
		attribute.String("job_id", id),
	))
	defer span.End()

	return s.db.Delete([]byte(id), pebble.Sync)
}

func (s *Store) Close() error {
	return s.db.Close()
}
