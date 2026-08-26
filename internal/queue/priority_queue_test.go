package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"gopherqueue/internal/task"
)

func TestPriorityOrdering(t *testing.T) {
	pq := NewPriorityQueue()

	// Enqueue in mixed priority order
	pq.Enqueue(task.NewTask("low", task.PriorityLow, nil))
	pq.Enqueue(task.NewTask("high", task.PriorityHigh, nil))
	pq.Enqueue(task.NewTask("med", task.PriorityMedium, nil))

	ctx := context.Background()

	first, err := pq.Dequeue(ctx)
	if err != nil || first.Priority != task.PriorityHigh {
		t.Fatalf("Expected HIGH priority task first, got %v (err: %v)", first.Priority, err)
	}

	second, err := pq.Dequeue(ctx)
	if err != nil || second.Priority != task.PriorityMedium {
		t.Fatalf("Expected MEDIUM priority task second, got %v (err: %v)", second.Priority, err)
	}

	third, err := pq.Dequeue(ctx)
	if err != nil || third.Priority != task.PriorityLow {
		t.Fatalf("Expected LOW priority task third, got %v (err: %v)", third.Priority, err)
	}
}

func TestFIFOWithSamePriority(t *testing.T) {
	pq := NewPriorityQueue()

	t1 := task.NewTask("first", task.PriorityMedium, nil)
	time.Sleep(2 * time.Millisecond) // slight delay to guarantee earlier timestamp
	t2 := task.NewTask("second", task.PriorityMedium, nil)

	pq.Enqueue(t1)
	pq.Enqueue(t2)

	ctx := context.Background()
	res1, _ := pq.Dequeue(ctx)
	res2, _ := pq.Dequeue(ctx)

	if res1.ID != t1.ID {
		t.Errorf("Expected first task to be dequeued first, got %s", res1.ID)
	}
	if res2.ID != t2.ID {
		t.Errorf("Expected second task to be dequeued second, got %s", res2.ID)
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	pq := NewPriorityQueue()
	numProducers := 10
	numConsumers := 5
	tasksPerProducer := 50
	totalTasks := numProducers * tasksPerProducer

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerProducer; j++ {
				_ = pq.Enqueue(task.NewTask("concurrent_job", task.PriorityMedium, nil))
			}
		}()
	}

	// Consumers
	consumed := make(chan *task.Task, totalTasks)
	for i := 0; i < numConsumers; i++ {
		go func() {
			for {
				t, err := pq.Dequeue(ctx)
				if err != nil {
					return
				}
				consumed <- t
			}
		}()
	}

	wg.Wait()

	// Wait to collect all consumed items
	receivedCount := 0
	for receivedCount < totalTasks {
		select {
		case <-consumed:
			receivedCount++
		case <-time.After(3 * time.Second):
			t.Fatalf("Timed out waiting for consumers. Received %d / %d", receivedCount, totalTasks)
		}
	}

	if receivedCount != totalTasks {
		t.Errorf("Expected %d consumed tasks, got %d", totalTasks, receivedCount)
	}
}

func TestQueueClose(t *testing.T) {
	pq := NewPriorityQueue()

	errCh := make(chan error, 1)
	go func() {
		_, err := pq.Dequeue(context.Background())
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	pq.Close()

	select {
	case err := <-errCh:
		if err != ErrQueueClosed {
			t.Errorf("Expected ErrQueueClosed, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Dequeue did not unblock upon Close()")
	}
}
