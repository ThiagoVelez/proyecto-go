package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"gopherqueue/internal/api"
	"gopherqueue/internal/handlers"
	"gopherqueue/internal/pool"
	"gopherqueue/internal/queue"
	"gopherqueue/internal/store"
)

const banner = `
   ______             __               ____                            
  / ____/____  ____  / /_  ___  _____ / __ \__  _____  __  _____       
 / / __ / __ \/ __ \/ __ \/ _ \/ ___// / / / / / / _ \/ / / / _ \      
/ /_/ // /_/ / /_/ / / / /  __/ /   / /_/ / /_/ /  __/ /_/ /  __/      
\____/ \____/ .___/_/ /_/\___/_/    \___\_\__,_/\___/\__,_/\___/       
           /_/                                                         
  >> Concurrent Task Queue & Worker Pool Engine in Go <<
`

func main() {
	port := flag.Int("port", 8080, "HTTP server listening port")
	initialWorkers := flag.Int("workers", 4, "Initial number of concurrent worker goroutines")
	flag.Parse()

	fmt.Print(banner)
	log.Printf("[INIT] Starting GopherQueue on port %d with %d initial workers...", *port, *initialWorkers)

	// 1. Initialize core components
	taskQueue := queue.NewPriorityQueue()
	taskStore := store.NewStore()
	registry := handlers.NewRegistry()

	// 2. Initialize Worker Pool
	cfg := pool.Config{
		InitialWorkers: *initialWorkers,
	}
	workerPool, err := pool.NewPool(cfg, taskQueue, taskStore, registry)
	if err != nil {
		log.Fatalf("[FATAL] Failed to create worker pool: %v", err)
	}

	// 3. Locate web assets directory
	webDir := "web"
	if _, err := os.Stat(webDir); os.IsNotExist(err) {
		// Fallback if running from a subdirectory
		webDir = filepath.Join("..", "..", "web")
	}

	// 4. Initialize HTTP API Server
	apiServer := api.NewServer(workerPool, taskStore, taskQueue, registry, webDir)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      apiServer,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 5. Start HTTP Server in background goroutine
	go func() {
		log.Printf("[HTTP] Dashboard & REST API available at: http://localhost:%d", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	// 6. Graceful Shutdown listener on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("\n[SHUTDOWN] Received interrupt signal. Initiating graceful shutdown...")

	// Create shutdown context with 15s timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	// A. Stop HTTP server from accepting new requests
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] HTTP server shutdown error: %v", err)
	}

	// B. Wait for worker pool to finish active tasks
	log.Println("[SHUTDOWN] Waiting for active worker goroutines to complete in-flight tasks...")
	if err := workerPool.Shutdown(10 * time.Second); err != nil {
		log.Printf("[WARN] Worker pool shutdown error: %v", err)
	} else {
		log.Println("[SHUTDOWN] All workers stopped cleanly.")
	}

	log.Println("[SHUTDOWN] GopherQueue exited successfully. Goodbye!")
}
