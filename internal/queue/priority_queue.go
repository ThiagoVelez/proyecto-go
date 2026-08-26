package queue

import (
	"container/heap"
	"context"
	"errors"
	"sync"

	"gopherqueue/internal/task"
)

var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueEmpty  = errors.New("queue is empty")
)

// taskHeap implements heap.Interface and holds Tasks.
type taskHeap []*task.Task

func (th taskHeap) Len() int { return len(th) }

func (th taskHeap) Less(i, j int) bool {
	// Lower numerical priority value means higher actual priority (1 > 2 > 3)
	if th[i].Priority != th[j].Priority {
		return th[i].Priority < th[j].Priority
	}
	// Tie-breaker: FIFO by creation time
	return th[i].CreatedAt.Before(th[j].CreatedAt)
}

func (th taskHeap) Swap(i, j int) {
	th[i], th[j] = th[j], th[i]
	th[i].Index = i
	th[j].Index = j
}

func (th *taskHeap) Push(x interface{}) {
	n := len(*th)
	item := x.(*task.Task)
	item.Index = n
	*th = append(*th, item)
}

func (th *taskHeap) Pop() interface{} {
	old := *th
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.Index = -1
	*th = old[0 : n-1]
	return item
}

// PriorityQueue provides a thread-safe, blocking priority queue for tasks.
type PriorityQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  taskHeap
	closed bool
}

// NewPriorityQueue initializes an empty thread-safe priority queue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: make(taskHeap, 0),
	}
	pq.cond = sync.NewCond(&pq.mu)
	heap.Init(&pq.items)
	return pq
}

// Enqueue adds a task to the queue and wakes up any waiting worker.
func (pq *PriorityQueue) Enqueue(t *task.Task) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return ErrQueueClosed
	}

	heap.Push(&pq.items, t)
	pq.cond.Signal() // Signal one waiting worker
	return nil
}

// Dequeue retrieves the highest priority task from the queue.
// It blocks until a task is available, the context is cancelled, or the queue is closed.
func (pq *PriorityQueue) Dequeue(ctx context.Context) (*task.Task, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Channel to handle context cancellation while waiting on condition
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			pq.mu.Lock()
			pq.cond.Broadcast()
			pq.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	for pq.items.Len() == 0 {
		if pq.closed {
			return nil, ErrQueueClosed
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		pq.cond.Wait()
	}

	if pq.closed && pq.items.Len() == 0 {
		return nil, ErrQueueClosed
	}

	item := heap.Pop(&pq.items).(*task.Task)
	return item, nil
}

// TryDequeue attempts to dequeue a task without blocking.
func (pq *PriorityQueue) TryDequeue() (*task.Task, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return nil, ErrQueueClosed
	}
	if pq.items.Len() == 0 {
		return nil, ErrQueueEmpty
	}

	item := heap.Pop(&pq.items).(*task.Task)
	return item, nil
}

// Len returns the current number of tasks in the queue.
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.items.Len()
}

// Close closes the queue and unblocks all waiting goroutines.
func (pq *PriorityQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.closed = true
	pq.cond.Broadcast()
}

// Snapshot returns a copy of the current tasks in the queue for inspection.
func (pq *PriorityQueue) Snapshot() []*task.Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	snapshot := make([]*task.Task, len(pq.items))
	for i, item := range pq.items {
		snapshot[i] = item.Clone()
	}
	return snapshot
}
