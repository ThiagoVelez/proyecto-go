package handlers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"gopherqueue/internal/task"
)

var (
	ErrHandlerNotFound = errors.New("handler not found for task type")
	ErrInvalidPayload  = errors.New("invalid payload parameters")
)

// HandlerFunc is the signature for task execution handlers.
type HandlerFunc func(ctx context.Context, t *task.Task) (interface{}, error)

// Registry manages the registration and lookup of task handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewRegistry creates a new handler registry with built-in default task handlers.
func NewRegistry() *Registry {
	r := &Registry{
		handlers: make(map[string]HandlerFunc),
	}
	r.registerBuiltins()
	return r
}

// Register registers a handler for a given task type.
func (r *Registry) Register(taskType string, handler HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = handler
}

// Get retrieves a handler for the given task type.
func (r *Registry) Get(taskType string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, exists := r.handlers[taskType]
	return h, exists
}

// Types returns all registered task types.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		types = append(types, k)
	}
	return types
}

func (r *Registry) registerBuiltins() {
	// 1. Send Email Handler
	r.Register("send_email", func(ctx context.Context, t *task.Task) (interface{}, error) {
		to, _ := t.Payload["to"].(string)
		subject, _ := t.Payload["subject"].(string)
		if to == "" {
			return nil, fmt.Errorf("%w: 'to' email address required", ErrInvalidPayload)
		}

		// Simulate processing delay (e.g. SMTP handshake)
		select {
		case <-time.After(800 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return map[string]interface{}{
			"recipient": to,
			"subject":   subject,
			"delivered": true,
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}, nil
	})

	// 2. Image Processing / Resizing Handler
	r.Register("image_resize", func(ctx context.Context, t *task.Task) (interface{}, error) {
		filename, _ := t.Payload["filename"].(string)
		width, _ := t.Payload["width"].(float64)
		height, _ := t.Payload["height"].(float64)

		if filename == "" {
			filename = "sample.jpg"
		}
		if width == 0 {
			width = 800
		}
		if height == 0 {
			height = 600
		}

		// Simulate heavy CPU computation
		select {
		case <-time.After(1200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return map[string]interface{}{
			"original_file": filename,
			"output_file":   fmt.Sprintf("thumb_%dx%d_%s", int(width), int(height), filename),
			"dimensions":    fmt.Sprintf("%dx%d", int(width), int(height)),
			"bytes_saved":   "42.5%",
		}, nil
	})

	// 3. Heavy Data Analytics / Report Generator
	r.Register("generate_report", func(ctx context.Context, t *task.Task) (interface{}, error) {
		reportType, _ := t.Payload["report_type"].(string)
		if reportType == "" {
			reportType = "monthly_summary"
		}

		// Simulate multi-step data aggregation
		select {
		case <-time.After(2000 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return map[string]interface{}{
			"report_name":     fmt.Sprintf("report_%s_%d.pdf", reportType, time.Now().Unix()),
			"records_scanned": 150420,
			"status":          "generated",
			"download_url":    fmt.Sprintf("/reports/%s.pdf", t.ID),
		}, nil
	})

	// 4. Flaky Task (Simulates intermittent failures to test exponential backoff retries)
	r.Register("flaky_task", func(ctx context.Context, t *task.Task) (interface{}, error) {
		failCountTarget := 2
		if fc, ok := t.Payload["fail_times"].(float64); ok && fc > 0 {
			failCountTarget = int(fc)
		}

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		if t.CurrentRetries < failCountTarget {
			return nil, fmt.Errorf("transient network timeout (attempt %d/%d)", t.CurrentRetries+1, failCountTarget)
		}

		return map[string]interface{}{
			"recovered":        true,
			"attempts_taken":   t.CurrentRetries + 1,
			"message":          "Task succeeded after exponential backoff retries!",
		}, nil
	})

	// 5. Quick Compute / Fast Job
	r.Register("quick_job", func(ctx context.Context, t *task.Task) (interface{}, error) {
		jitter := time.Duration(100+rand.Intn(300)) * time.Millisecond
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return map[string]interface{}{
			"processed": true,
			"latency":   jitter.String(),
		}, nil
	})
}
