package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gopherqueue/internal/handlers"
	"gopherqueue/internal/queue"
	"gopherqueue/internal/store"
	"gopherqueue/internal/task"
)

func setupTestPool(workers int) (*Pool, *store.Store, *queue.PriorityQueue, *handlers.Registry) {
	q := queue.NewPriorityQueue()
	s := store.NewStore()
	r := handlers.NewRegistry()

	cfg := Config{InitialWorkers: workers}
	p, _ := NewPool(cfg, q, s, r)
	return p, s, q, r
}

func TestWorkerPoolExecution(t *testing.T) {
	p, s, _, r := setupTestPool(2)
	defer p.Shutdown(2 * time.Second)

	var executed uint32
	r.Register("test_job", func(ctx context.Context, task *task.Task) (interface{}, error) {
		atomic.AddUint32(&executed, 1)
		return "ok", nil
	})

	tsk := task.NewTask("test_job", task.PriorityHigh, nil)
	if err := p.Submit(tsk); err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for task completion
	time.Sleep(300 * time.Millisecond)

	saved, found := s.Get(tsk.ID)
	if !found {
		t.Fatalf("Task not found in store")
	}

	if saved.Status != task.StatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", saved.Status)
	}
	if atomic.LoadUint32(&executed) != 1 {
		t.Errorf("Expected executed count 1, got %d", executed)
	}
}

func TestWorkerPoolScaling(t *testing.T) {
	p, _, _, _ := setupTestPool(2)
	defer p.Shutdown(2 * time.Second)

	if p.TotalWorkersCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", p.TotalWorkersCount())
	}

	// Scale UP to 5
	if err := p.Scale(5); err != nil {
		t.Fatalf("Failed to scale up: %v", err)
	}
	if p.TotalWorkersCount() != 5 {
		t.Errorf("Expected 5 workers after scale up, got %d", p.TotalWorkersCount())
	}

	// Scale DOWN to 1
	if err := p.Scale(1); err != nil {
		t.Fatalf("Failed to scale down: %v", err)
	}
	if p.TotalWorkersCount() != 1 {
		t.Errorf("Expected 1 worker after scale down, got %d", p.TotalWorkersCount())
	}
}

func TestPanicRecovery(t *testing.T) {
	p, s, _, r := setupTestPool(1)
	defer p.Shutdown(2 * time.Second)

	r.Register("panic_job", func(ctx context.Context, task *task.Task) (interface{}, error) {
		panic("simulated critical crash")
	})

	tsk := task.NewTask("panic_job", task.PriorityHigh, nil)
	tsk.MaxRetries = 0 // don't retry, fail directly
	_ = p.Submit(tsk)

	time.Sleep(300 * time.Millisecond)

	saved, _ := s.Get(tsk.ID)
	if saved.Status != task.StatusFailed {
		t.Errorf("Expected status FAILED after panic, got %s", saved.Status)
	}
}

func TestGracefulShutdown(t *testing.T) {
	p, s, _, r := setupTestPool(2)

	var inFlight uint32
	r.Register("slow_job", func(ctx context.Context, task *task.Task) (interface{}, error) {
		atomic.StoreUint32(&inFlight, 1)
		time.Sleep(400 * time.Millisecond)
		return "finished", nil
	})

	tsk := task.NewTask("slow_job", task.PriorityHigh, nil)
	_ = p.Submit(tsk)

	// Wait until task starts
	time.Sleep(50 * time.Millisecond)

	// Initiate shutdown while task is running
	shutdownStart := time.Now()
	err := p.Shutdown(2 * time.Second)
	shutdownDuration := time.Since(shutdownStart)

	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	if shutdownDuration < 300*time.Millisecond {
		t.Errorf("Shutdown exited prematurely before task completed (took %v)", shutdownDuration)
	}

	saved, _ := s.Get(tsk.ID)
	if saved.Status != task.StatusCompleted {
		t.Errorf("Expected task to complete before shutdown, got %s", saved.Status)
	}
}

func TestExponentialBackoffRetry(t *testing.T) {
	p, s, _, r := setupTestPool(2)
	defer p.Shutdown(5 * time.Second)

	var attempts int32
	r.Register("retry_test", func(ctx context.Context, task *task.Task) (interface{}, error) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 2 {
			return nil, errors.New("temporary failure")
		}
		return "success", nil
	})

	tsk := task.NewTask("retry_test", task.PriorityHigh, nil)
	tsk.MaxRetries = 2
	_ = p.Submit(tsk)

	// Worker should fail once, schedule retry (base delay ~2s), and succeed on attempt 2
	time.Sleep(3 * time.Second)

	saved, _ := s.Get(tsk.ID)
	if saved.Status != task.StatusCompleted {
		t.Errorf("Expected task to eventually succeed with retry, got status: %s (attempts: %d)", saved.Status, atomic.LoadInt32(&attempts))
	}
}
