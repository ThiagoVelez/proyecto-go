package store

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gopherqueue/internal/task"
)

// MetricsSnapshot contains aggregated performance statistics.
type MetricsSnapshot struct {
	TotalSubmitted         uint64  `json:"total_submitted"`
	TotalCompleted         uint64  `json:"total_completed"`
	TotalFailed            uint64  `json:"total_failed"`
	TotalRetried           uint64  `json:"total_retried"`
	PendingCount           int     `json:"pending_count"`
	RunningCount           int     `json:"running_count"`
	ActiveWorkers          int     `json:"active_workers"`
	TotalWorkers           int     `json:"total_workers"`
	AverageDurationMs      float64 `json:"avg_duration_ms"`
	SuccessRatePercent     float64 `json:"success_rate_percent"`
	ThroughputPerMinute    float64 `json:"throughput_per_minute"`
}

// Store provides thread-safe storage and metrics calculation for tasks.
type Store struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task

	// Atomic counters for high-throughput metrics
	totalSubmitted uint64
	totalCompleted uint64
	totalFailed    uint64
	totalRetried   uint64
	totalTimeMs    int64

	startTime time.Time
}

// NewStore initializes a new in-memory task store.
func NewStore() *Store {
	return &Store{
		tasks:     make(map[string]*task.Task),
		startTime: time.Now(),
	}
}

// Save stores or updates a task snapshot.
func (s *Store) Save(t *task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[t.ID] = t.Clone()
}

// IncrementSubmitted increments the total submitted counter.
func (s *Store) IncrementSubmitted() {
	atomic.AddUint64(&s.totalSubmitted, 1)
}

// Get retrieves a task by ID.
func (s *Store) Get(id string) (*task.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, found := s.tasks[id]
	if !found {
		return nil, false
	}
	return t.Clone(), true
}

// List returns recent tasks filtered optionally by status.
func (s *Store) List(limit int, status task.TaskStatus) []*task.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*task.Task
	for _, t := range s.tasks {
		if status == "" || t.Status == status {
			result = append(result, t.Clone())
		}
	}

	// Sort newest first
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

// RecordCompletion updates stats upon task completion.
func (s *Store) RecordCompletion(t *task.Task) {
	atomic.AddUint64(&s.totalCompleted, 1)
	atomic.AddInt64(&s.totalTimeMs, t.DurationMs)
	s.Save(t)
}

// RecordFailure updates stats upon permanent task failure.
func (s *Store) RecordFailure(t *task.Task) {
	atomic.AddUint64(&s.totalFailed, 1)
	s.Save(t)
}

// RecordRetry updates stats upon a retry attempt.
func (s *Store) RecordRetry(t *task.Task) {
	atomic.AddUint64(&s.totalRetried, 1)
	s.Save(t)
}

// GetMetrics returns a real-time snapshot of the system metrics.
func (s *Store) GetMetrics(activeWorkers, totalWorkers, pendingQueueLen int) MetricsSnapshot {
	s.mu.RLock()
	runningCount := 0
	for _, t := range s.tasks {
		if t.Status == task.StatusRunning {
			runningCount++
		}
	}
	s.mu.RUnlock()

	completed := atomic.LoadUint64(&s.totalCompleted)
	failed := atomic.LoadUint64(&s.totalFailed)
	totalDone := completed + failed
	totalMs := atomic.LoadInt64(&s.totalTimeMs)

	avgDuration := 0.0
	if completed > 0 {
		avgDuration = float64(totalMs) / float64(completed)
	}

	successRate := 100.0
	if totalDone > 0 {
		successRate = (float64(completed) / float64(totalDone)) * 100.0
	}

	minutesElapsed := time.Since(s.startTime).Minutes()
	throughput := 0.0
	if minutesElapsed > 0 {
		throughput = float64(totalDone) / minutesElapsed
	}

	return MetricsSnapshot{
		TotalSubmitted:      atomic.LoadUint64(&s.totalSubmitted),
		TotalCompleted:      completed,
		TotalFailed:         failed,
		TotalRetried:        atomic.LoadUint64(&s.totalRetried),
		PendingCount:        pendingQueueLen,
		RunningCount:        runningCount,
		ActiveWorkers:       activeWorkers,
		TotalWorkers:        totalWorkers,
		AverageDurationMs:   avgDuration,
		SuccessRatePercent:  successRate,
		ThroughputPerMinute: throughput,
	}
}
