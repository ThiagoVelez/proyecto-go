package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gopherqueue/internal/handlers"
	"gopherqueue/internal/pool"
	"gopherqueue/internal/queue"
	"gopherqueue/internal/store"
	"gopherqueue/internal/task"
)

// Server handles HTTP API requests and serves the monitoring dashboard.
type Server struct {
	pool      *pool.Pool
	store     *store.Store
	queue     *queue.PriorityQueue
	registry  *handlers.Registry
	staticDir string
	mux       *http.ServeMux
}

// NewServer initializes a new API server.
func NewServer(
	p *pool.Pool,
	s *store.Store,
	q *queue.PriorityQueue,
	reg *handlers.Registry,
	staticDir string,
) *Server {
	srv := &Server{
		pool:      p,
		store:     s,
		queue:     q,
		registry:  reg,
		staticDir: staticDir,
		mux:       http.NewServeMux(),
	}
	srv.routes()
	return srv
}

func (s *Server) routes() {
	// API Endpoints
	s.mux.HandleFunc("/api/tasks", s.handleTasks)
	s.mux.HandleFunc("/api/tasks/", s.handleTaskByID)
	s.mux.HandleFunc("/api/tasks/batch", s.handleBatchTasks)
	s.mux.HandleFunc("/api/metrics", s.handleMetrics)
	s.mux.HandleFunc("/api/workers", s.handleWorkers)
	s.mux.HandleFunc("/api/workers/scale", s.handleScaleWorkers)
	s.mux.HandleFunc("/api/task-types", s.handleTaskTypes)

	// Static Dashboard Web UI
	if s.staticDir != "" {
		fs := http.FileServer(http.Dir(s.staticDir))
		s.mux.Handle("/", fs)
	}
}

// ServeHTTP implements http.Handler with CORS and basic logging middleware.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS for local development
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	start := time.Now()
	s.mux.ServeHTTP(w, r)
	if strings.HasPrefix(r.URL.Path, "/api/") {
		log.Printf("[HTTP] %s %s (%v)", r.Method, r.URL.Path, time.Since(start))
	}
}

type createTaskRequest struct {
	Type           string                 `json:"type"`
	Priority       int                    `json:"priority"`
	Payload        map[string]interface{} `json:"payload"`
	TimeoutSeconds int                    `json:"timeout_seconds,omitempty"`
	MaxRetries     int                    `json:"max_retries,omitempty"`
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req createTaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.jsonError(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}

		if req.Type == "" {
			s.jsonError(w, "field 'type' is required", http.StatusBadRequest)
			return
		}

		p := task.Priority(req.Priority)
		if p < task.PriorityHigh || p > task.PriorityLow {
			p = task.PriorityMedium
		}

		t := task.NewTask(req.Type, p, req.Payload)
		if req.TimeoutSeconds > 0 {
			t.Timeout = time.Duration(req.TimeoutSeconds) * time.Second
		}
		if req.MaxRetries > 0 {
			t.MaxRetries = req.MaxRetries
		}

		if err := s.pool.Submit(t); err != nil {
			s.jsonError(w, fmt.Sprintf("failed to submit task: %v", err), http.StatusInternalServerError)
			return
		}

		s.jsonResponse(w, http.StatusCreated, t)

	case http.MethodGet:
		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		statusFilter := task.TaskStatus(r.URL.Query().Get("status"))

		tasks := s.store.List(limit, statusFilter)
		s.jsonResponse(w, http.StatusOK, tasks)

	default:
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if id == "" {
		s.jsonError(w, "task ID required", http.StatusBadRequest)
		return
	}

	t, found := s.store.Get(id)
	if !found {
		s.jsonError(w, "task not found", http.StatusNotFound)
		return
	}

	s.jsonResponse(w, http.StatusOK, t)
}

type batchCreateRequest struct {
	Count    int                    `json:"count"`
	Type     string                 `json:"type"`
	Priority int                    `json:"priority"`
	Payload  map[string]interface{} `json:"payload"`
}

func (s *Server) handleBatchTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req batchCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Count <= 0 {
		req.Count = 10
	}
	if req.Count > 500 {
		req.Count = 500 // safety limit
	}
	if req.Type == "" {
		req.Type = "quick_job"
	}

	p := task.Priority(req.Priority)
	if p < task.PriorityHigh || p > task.PriorityLow {
		p = task.PriorityMedium
	}

	var createdIDs []string
	for i := 0; i < req.Count; i++ {
		payloadCopy := make(map[string]interface{})
		for k, v := range req.Payload {
			payloadCopy[k] = v
		}
		payloadCopy["batch_index"] = i + 1

		t := task.NewTask(req.Type, p, payloadCopy)
		if err := s.pool.Submit(t); err != nil {
			s.jsonError(w, fmt.Sprintf("failed to submit batch task: %v", err), http.StatusInternalServerError)
			return
		}
		createdIDs = append(createdIDs, t.ID)
	}

	s.jsonResponse(w, http.StatusCreated, map[string]interface{}{
		"submitted_count": len(createdIDs),
		"task_ids":        createdIDs,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	activeWorkers := s.pool.ActiveWorkersCount()
	totalWorkers := s.pool.TotalWorkersCount()
	queueLen := s.queue.Len()

	metrics := s.store.GetMetrics(activeWorkers, totalWorkers, queueLen)
	s.jsonResponse(w, http.StatusOK, metrics)
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	workers := s.pool.WorkersInfo()
	s.jsonResponse(w, http.StatusOK, workers)
}

type scaleRequest struct {
	Count int `json:"count"`
}

func (s *Server) handleScaleWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req scaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Count <= 0 || req.Count > 100 {
		s.jsonError(w, "worker count must be between 1 and 100", http.StatusBadRequest)
		return
	}

	if err := s.pool.Scale(req.Count); err != nil {
		s.jsonError(w, fmt.Sprintf("failed to scale workers: %v", err), http.StatusInternalServerError)
		return
	}

	s.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"message":       fmt.Sprintf("Worker pool scaled to %d workers", req.Count),
		"total_workers": req.Count,
	})
}

func (s *Server) handleTaskTypes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	types := s.registry.Types()
	s.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"available_types": types,
	})
}

func (s *Server) jsonResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) jsonError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
