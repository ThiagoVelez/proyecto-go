package pool

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"gopherqueue/internal/handlers"
	"gopherqueue/internal/queue"
	"gopherqueue/internal/store"
	"gopherqueue/internal/task"
)

// WorkerInfo provides a public snapshot of worker status.
type WorkerInfo struct {
	ID             int        `json:"id"`
	Status         string     `json:"status"` // "IDLE", "BUSY", "STOPPED"
	CurrentTaskID  string     `json:"current_task_id,omitempty"`
	CurrentType    string     `json:"current_type,omitempty"`
	TasksProcessed uint64     `json:"tasks_processed"`
	CurrentStarted *time.Time `json:"current_started,omitempty"`
}

// Worker represents a single worker goroutine processing tasks.
type Worker struct {
	id             int
	queue          *queue.PriorityQueue
	store          *store.Store
	registry       *handlers.Registry
	retryChan      chan<- *task.Task
	activeCounter  *int32

	mu             sync.RWMutex
	status         string
	currentTask    *task.Task
	tasksProcessed uint64
	stopChan       chan struct{}
}

// NewWorker initializes a new Worker.
func NewWorker(
	id int,
	q *queue.PriorityQueue,
	s *store.Store,
	reg *handlers.Registry,
	retryChan chan<- *task.Task,
	activeCounter *int32,
) *Worker {
	return &Worker{
		id:            id,
		queue:         q,
		store:         s,
		registry:      reg,
		retryChan:     retryChan,
		activeCounter: activeCounter,
		status:        "IDLE",
		stopChan:      make(chan struct{}),
	}
}

// Start runs the worker loop in a new goroutine.
func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			w.mu.Lock()
			w.status = "STOPPED"
			w.mu.Unlock()
		}()

		for {
			w.mu.Lock()
			w.status = "IDLE"
			w.currentTask = nil
			w.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-w.stopChan:
				return
			default:
			}

			// Block until a task is available or context cancelled
			t, err := w.queue.Dequeue(ctx)
			if err != nil {
				// Queue closed or context cancelled
				return
			}

			w.executeTask(ctx, t)
		}
	}()
}

// Stop signals this worker to exit gracefully after finishing current task.
func (w *Worker) Stop() {
	close(w.stopChan)
}

func (w *Worker) executeTask(parentCtx context.Context, t *task.Task) {
	atomic.AddInt32(w.activeCounter, 1)
	defer atomic.AddInt32(w.activeCounter, -1)

	now := time.Now().UTC()
	t.Status = task.StatusRunning
	t.StartedAt = &now
	w.store.Save(t)

	w.mu.Lock()
	w.status = "BUSY"
	w.currentTask = t
	w.mu.Unlock()

	// Task timeout context
	taskTimeout := t.Timeout
	if taskTimeout <= 0 {
		taskTimeout = 30 * time.Second
	}
	taskCtx, cancel := context.WithTimeout(parentCtx, taskTimeout)
	defer cancel()

	start := time.Now()
	var (
		result interface{}
		err    error
	)

	// Safe execution with recover to handle panics in job handlers
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task panicked: %v\nstack: %s", r, string(debug.Stack()))
			}
		}()

		handler, found := w.registry.Get(t.Type)
		if !found {
			err = fmt.Errorf("%w: '%s'", handlers.ErrHandlerNotFound, t.Type)
			return
		}

		result, err = handler(taskCtx, t)
	}()

	duration := time.Since(start)
	t.DurationMs = duration.Milliseconds()
	completedTime := time.Now().UTC()
	t.CompletedAt = &completedTime

	if err != nil {
		t.Error = err.Error()

		if t.CurrentRetries < t.MaxRetries && parentCtx.Err() == nil {
			// Trigger exponential backoff retry
			t.CurrentRetries++
			t.Status = task.StatusRetrying
			w.store.RecordRetry(t)

			// Send to retry scheduler
			go w.scheduleRetry(parentCtx, t)
		} else {
			// Max retries exceeded or parent context cancelled
			t.Status = task.StatusFailed
			w.store.RecordFailure(t)
		}
	} else {
		t.Status = task.StatusCompleted
		t.Result = result
		t.Error = ""
		w.store.RecordCompletion(t)
	}

	w.mu.Lock()
	w.tasksProcessed++
	w.mu.Unlock()
}

// scheduleRetry calculates exponential backoff delay with jitter and re-enqueues the task.
func (w *Worker) scheduleRetry(ctx context.Context, t *task.Task) {
	// Base backoff: 2^attempt seconds (e.g. 2s, 4s, 8s) + random jitter (0-500ms)
	backoffSec := math.Pow(2, float64(t.CurrentRetries))
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	delay := time.Duration(backoffSec)*time.Second + jitter

	select {
	case <-time.After(delay):
		t.Status = task.StatusPending
		w.store.Save(t)
		_ = w.queue.Enqueue(t)
	case <-ctx.Done():
		t.Status = task.StatusCancelled
		t.Error = "retry cancelled: system shutting down"
		w.store.RecordFailure(t)
	}
}

// Info returns the current snapshot of worker status.
func (w *Worker) Info() WorkerInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	info := WorkerInfo{
		ID:             w.id,
		Status:         w.status,
		TasksProcessed: w.tasksProcessed,
	}

	if w.currentTask != nil {
		info.CurrentTaskID = w.currentTask.ID
		info.CurrentType = w.currentTask.Type
		info.CurrentStarted = w.currentTask.StartedAt
	}

	return info
}
