package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"log"
	"fmt"
	"os/signal"
	"syscall"
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

	// Create 3 Workers
	worker1 := broker.Worker{
		ID: "1",
		Broker: b,
		Handler: func(job broker.Job) error{
			fmt.Printf("Processing job: %s\n", job.ID)
			return nil
		},
	}

	worker2 := broker.Worker{
		ID: "2",
		Broker: b,
		Handler: func(job broker.Job) error{
			fmt.Printf("Processing job: %s\n", job.ID)
			return nil
		},
	}

	worker3 := broker.Worker{
		ID: "3",
		Broker: b,
		Handler: func(job broker.Job) error{
			fmt.Printf("Processing job: %s\n", job.ID)
			return nil
		},
	}

	// Create the Reaper
	reaper := broker.Reaper{
		Broker: b,
	}

	// Create 6 Test Jobs 
	testJob1 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob1)
	if err != nil{
		log.Printf("Failed to submit the TestJob1: %v\n", err)
		return
	}

	testJob2 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-201", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-201",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob2)
	if err != nil{
		log.Printf("Failed to submit the TestJob2: %v\n", err)
		return
	}

	testJob3 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-301", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-301",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob3)
	if err != nil{
		log.Printf("Failed to submit the TestJob3: %v\n", err)
		return
	}

	testJob4 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-401", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-401",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob4)
	if err != nil{
		log.Printf("Failed to submit the TestJob4: %v\n", err)
		return
	}

	testJob5 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-501", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-501",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob5)
	if err != nil{
		log.Printf("Failed to submit the TestJob5: %v\n", err)
		return
	}

	testJob6 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-601", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-601",
	}

	// Call the Submit function and handle potential error
	err = worker1.Broker.Submit(ctx, testJob6)
	if err != nil{
		log.Printf("Failed to submit the TestJob6: %v\n", err)
		return
	}

	// Start the go routines for each worker
	go worker1.Start(ctx)
	go worker2.Start(ctx)
	go worker3.Start(ctx)

	// Start the go routine for the reaper
	go reaper.Begin(ctx)
	<-ctx.Done()
	log.Println("Shutting down...")
}
