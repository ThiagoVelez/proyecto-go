package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gopherqueue/internal/handlers"
	"gopherqueue/internal/queue"
	"gopherqueue/internal/store"
	"gopherqueue/internal/task"
)

var (
	ErrPoolStopped    = errors.New("worker pool is stopped")
	ErrInvalidWorkers = errors.New("worker count must be greater than 0")
)

// Config defines the configuration for creating a Worker Pool.
type Config struct {
	InitialWorkers int
	MaxQueueSize   int
}

// Pool coordinates the worker goroutines, priority queue, and task store.
type Pool struct {
	queue    *queue.PriorityQueue
	store    *store.Store
	registry *handlers.Registry

	mu            sync.Mutex
	workers       map[int]*Worker
	nextWorkerID  int
	activeWorkers int32
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	stopped       bool
}

// NewPool creates a new Worker Pool.
func NewPool(cfg Config, q *queue.PriorityQueue, s *store.Store, reg *handlers.Registry) (*Pool, error) {
	if cfg.InitialWorkers <= 0 {
		cfg.InitialWorkers = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		queue:        q,
		store:        s,
		registry:     reg,
		workers:      make(map[int]*Worker),
		nextWorkerID: 1,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Spawn initial workers
	for i := 0; i < cfg.InitialWorkers; i++ {
		p.addWorker()
	}

	return p, nil
}

// Submit adds a new task to the queue and records it in the store.
func (p *Pool) Submit(t *task.Task) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.mu.Unlock()

	p.store.IncrementSubmitted()
	p.store.Save(t)

	if err := p.queue.Enqueue(t); err != nil {
		t.Status = task.StatusFailed
		t.Error = fmt.Sprintf("failed to enqueue: %v", err)
		p.store.RecordFailure(t)
		return err
	}

	return nil
}

// SubmitBatch submits multiple tasks to the pool.
func (p *Pool) SubmitBatch(tasks []*task.Task) error {
	for _, t := range tasks {
		if err := p.Submit(t); err != nil {
			return err
		}
	}
	return nil
}

// addWorker spawns a new worker (must be called with p.mu locked or during initialization).
func (p *Pool) addWorker() *Worker {
	id := p.nextWorkerID
	p.nextWorkerID++

	w := NewWorker(id, p.queue, p.store, p.registry, nil, &p.activeWorkers)
	p.workers[id] = w
	w.Start(p.ctx, &p.wg)
	return w
}

// Scale dynamically adjusts the number of active workers in the pool.
func (p *Pool) Scale(targetCount int) error {
	if targetCount <= 0 {
		return ErrInvalidWorkers
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return ErrPoolStopped
	}

	currentCount := len(p.workers)
	if targetCount == currentCount {
		return nil
	}

	if targetCount > currentCount {
		// Scale UP
		toAdd := targetCount - currentCount
		for i := 0; i < toAdd; i++ {
			p.addWorker()
		}
	} else {
		// Scale DOWN (stop excess workers gracefully)
		toRemove := currentCount - targetCount
		count := 0
		for id, w := range p.workers {
			if count >= toRemove {
				break
			}
			w.Stop()
			delete(p.workers, id)
			count++
		}
	}

	return nil
}

// WorkersInfo returns a snapshot list of current workers and their status.
func (p *Pool) WorkersInfo() []WorkerInfo {
	p.mu.Lock()
	workers := make([]*Worker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
	}
	p.mu.Unlock()

	infos := make([]WorkerInfo, len(workers))
	for i, w := range workers {
		infos[i] = w.Info()
	}
	return infos
}

// ActiveWorkersCount returns the number of workers currently processing tasks.
func (p *Pool) ActiveWorkersCount() int {
	return int(atomic.LoadInt32(&p.activeWorkers))
}

// TotalWorkersCount returns total allocated workers.
func (p *Pool) TotalWorkersCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// Shutdown initiates graceful termination of the worker pool.
// It waits for active tasks to finish up to the specified timeout.
func (p *Pool) Shutdown(timeout time.Duration) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()

	// 1. Close queue to stop accepting new tasks and unblock workers waiting on empty queue
	p.queue.Close()

	// 2. Wait for workers to finish in-flight tasks or timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cancel()
		return nil
	case <-time.After(timeout):
		// Force cancellation if timeout exceeded
		p.cancel()
		return fmt.Errorf("graceful shutdown timed out after %v", timeout)
	}
}
