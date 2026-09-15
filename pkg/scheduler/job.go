package scheduler

import "time"

// Job represents a task to be executed
type Job struct {
	ID             string        `json:"id"`
	Payload        string        `json:"payload"`
	CronExpr       string        `json:"cron_expr,omitempty"`
	NextRun        time.Time     `json:"next_run"`
	RepeatInterval time.Duration `json:"repeat_interval,omitempty"`
	MaxRuns        int           `json:"max_runs,omitempty"`
	RunCount       int           `json:"run_count"`
	RateLimitKey   string        `json:"rate_limit_key,omitempty"`
	Weight         int           `json:"weight,omitempty"`

	// Retry Policy
	MaxRetries     int           `json:"max_retries,omitempty"`     // Max retry attempts upon execution failure
	RetryCount     int           `json:"retry_count,omitempty"`     // Current number of retries attempted
	InitialBackoff time.Duration `json:"initial_backoff,omitempty"` // Initial backoff delay (default 1s if unset)
	MaxBackoff     time.Duration `json:"max_backoff,omitempty"`     // Maximum cap for exponential backoff (default 1m if unset)
}

// PriorityQueue implements heap.Interface
type JobQueue []*Job

func (jq JobQueue) Len() int           { return len(jq) }
func (jq JobQueue) Less(i, j int) bool { return jq[i].NextRun.Before(jq[j].NextRun) }
func (jq JobQueue) Swap(i, j int)      { jq[i], jq[j] = jq[j], jq[i] }

func (jq *JobQueue) Push(x interface{}) {
	item := x.(*Job)
	*jq = append(*jq, item)
}

func (jq *JobQueue) Pop() interface{} {
	old := *jq
	n := len(old)
	item := old[n-1]
	*jq = old[0 : n-1]
	return item
}
