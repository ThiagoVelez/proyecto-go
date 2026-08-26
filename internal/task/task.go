package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Priority represents the priority level of a task (1 is highest, 3 is lowest).
type Priority int

const (
	PriorityHigh   Priority = 1
	PriorityMedium Priority = 2
	PriorityLow    Priority = 3
)

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "HIGH"
	case PriorityMedium:
		return "MEDIUM"
	case PriorityLow:
		return "LOW"
	default:
		return "UNKNOWN"
	}
}

// TaskStatus represents the current execution state of a task.
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"
	StatusRunning   TaskStatus = "RUNNING"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
	StatusRetrying  TaskStatus = "RETRYING"
	StatusCancelled TaskStatus = "CANCELLED"
)

// Task represents a unit of work to be processed by the worker pool.
type Task struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Priority       Priority               `json:"priority"`
	Payload        map[string]interface{} `json:"payload"`
	Status         TaskStatus             `json:"status"`
	MaxRetries     int                    `json:"max_retries"`
	CurrentRetries int                    `json:"current_retries"`
	Timeout        time.Duration          `json:"timeout"`
	Result         interface{}            `json:"result,omitempty"`
	Error          string                 `json:"error,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	StartedAt      *time.Time             `json:"started_at,omitempty"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
	DurationMs     int64                  `json:"duration_ms"`

	// index is maintained by the heap.Interface methods inside the priority queue.
	Index int `json:"-"`
}

// NewTask creates a new Task with default values and a generated UUID-like ID.
func NewTask(taskType string, priority Priority, payload map[string]interface{}) *Task {
	if priority < PriorityHigh || priority > PriorityLow {
		priority = PriorityMedium
	}
	if payload == nil {
		payload = make(map[string]interface{})
	}

	return &Task{
		ID:             generateID(),
		Type:           taskType,
		Priority:       priority,
		Payload:        payload,
		Status:         StatusPending,
		MaxRetries:     3,
		CurrentRetries: 0,
		Timeout:        30 * time.Second,
		CreatedAt:      time.Now().UTC(),
	}
}

func generateID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("task-%s", hex.EncodeToString(bytes))
}

// Clone creates a shallow copy of the task for thread-safe state snapshots.
func (t *Task) Clone() *Task {
	c := *t
	// Copy payload map
	if t.Payload != nil {
		c.Payload = make(map[string]interface{}, len(t.Payload))
		for k, v := range t.Payload {
			c.Payload[k] = v
		}
	}
	return &c
}

// PayloadJSON converts payload to JSON bytes.
func (t *Task) PayloadJSON() []byte {
	b, _ := json.Marshal(t.Payload)
	return b
}
