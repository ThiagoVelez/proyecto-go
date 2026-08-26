package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	serverURL := flag.String("url", "http://localhost:8080", "Target GopherQueue server URL")
	tasksCount := flag.Int("n", 100, "Total number of tasks to dispatch")
	concurrency := flag.Int("c", 10, "Number of concurrent dispatchers (goroutines)")
	taskType := flag.String("type", "quick_job", "Task type to submit")
	flag.Parse()

	fmt.Println("========================================================")
	fmt.Println("🚀 GopherQueue High-Concurrency CLI Benchmark Tool")
	fmt.Println("========================================================")
	fmt.Printf("Server URL:   %s\n", *serverURL)
	fmt.Printf("Total Tasks:  %d\n", *tasksCount)
	fmt.Printf("Concurrency:  %d client goroutines\n", *concurrency)
	fmt.Printf("Task Type:    %s\n\n", *taskType)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Verify server health
	resp, err := client.Get(*serverURL + "/api/metrics")
	if err != nil {
		log.Fatalf("❌ Error connecting to GopherQueue server at %s: %v\nMake sure the server is running (`go run cmd/server/main.go`)", *serverURL, err)
	}
	resp.Body.Close()

	start := time.Now()
	var (
		successCount uint64
		failCount    uint64
		wg           sync.WaitGroup
	)

	taskCh := make(chan int, *tasksCount)
	for i := 1; i <= *tasksCount; i++ {
		taskCh <- i
	}
	close(taskCh)

	// Launch concurrent dispatchers
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for taskNum := range taskCh {
				priority := (taskNum % 3) + 1
				payload := map[string]interface{}{
					"type":     *taskType,
					"priority": priority,
					"payload": map[string]interface{}{
						"task_number": taskNum,
						"cli_worker":  workerID,
						"timestamp":   time.Now().UnixNano(),
					},
				}
				body, _ := json.Marshal(payload)

				postResp, err := client.Post(
					*serverURL+"/api/tasks",
					"application/json",
					bytes.NewReader(body),
				)
				if err != nil || postResp.StatusCode >= 400 {
					atomic.AddUint64(&failCount, 1)
					if postResp != nil {
						io.Copy(io.Discard, postResp.Body)
						postResp.Body.Close()
					}
				} else {
					atomic.AddUint64(&successCount, 1)
					io.Copy(io.Discard, postResp.Body)
					postResp.Body.Close()
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	rps := float64(*tasksCount) / duration.Seconds()

	fmt.Println("\n📊 Benchmark Results:")
	fmt.Println("--------------------------------------------------------")
	fmt.Printf("Elapsed Time:      %v\n", duration)
	fmt.Printf("Tasks Dispatched:  %d (✅ %d OK / ❌ %d Failed)\n", *tasksCount, successCount, failCount)
	fmt.Printf("Dispatch Rate:     %.2f requests/sec\n", rps)
	fmt.Println("--------------------------------------------------------")
	fmt.Println("Check the Web Dashboard at http://localhost:8080 to watch tasks execute in real time!")
}
