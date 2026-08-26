package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"log"
	"fmt"
	"os/signal"
	"syscall"
	"time"
)

//Main function for testing our internal code
func main(){
	// Create a context that listens for SIGINT and SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() //cleans up resources when main exits

	fmt.Println("Application started. Press Ctrl+C to exit.")

	// Create a Broker by passing the context and connectionString
	b, err := broker.NewBroker(ctx, "postgres://abhirambanda:postgres@localhost:5434/distributed_job_queue")
	
	// Handle the broker creation error
	if err != nil{
		log.Printf("Failed to create broker: %v\n", err)
		return
	}

	// Create the Reaper
	reaper := broker.Reaper{
		Broker: b,
	}

	// Create n amount of test jobs - for dashboard and throughput numbers
	for i:=0; i < 100; i++{
		// Makes sure each has a unique idempotency key
		testJob1 := broker.Job{
			Payload: json.RawMessage(fmt.Sprintf(`{"type": "generate_note", "visit_id": "visit-%d", "template": "soap_note"}`, i)),
			IdempotencyKey: fmt.Sprintf("note-visit-%d", i),
		}
		
		err = b.Submit(ctx, testJob1)
		if err != nil{
			log.Printf(fmt.Sprintf("Failed to submit the TestJob%d: %v\n", i, err))
			return
		}
	}

	// Create n amount of workers to complete the jobs
	workers := make([]broker.Worker, 10)
	for i := 0; i < 10; i++ {
		workers[i] = broker.Worker{
			ID: fmt.Sprintf("%d", i+1),
			Broker: b,
			Handler: func(job broker.Job) error {
				fmt.Printf("Processing job: %s\n", job.ID)
				time.Sleep(200 * time.Millisecond)
				return nil
			},
		}
	}

	// Start each worker in its own go routine
	for i := range workers{
		go workers[i].Start(ctx)
	}

	// Keep track of the time to measure how long it takes to finish all jobs
	start := time.Now()

	// Loop which polls the db to query if all jobs were done. if the number matches, get the duration
	for {
		var count int
		b.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'completed'").Scan(&count)
		if count >= 100 {
			duration := time.Since(start)
			fmt.Printf("Processed 1000 jobs across 10 workers in %v\n", duration)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Start the go routine for the reaper
	go reaper.Begin(ctx)
	<-ctx.Done()
	
	log.Println("Shutting down...")
}
