package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"log"
)

//Main function for testing our internal code
func main(){
	// Create the context we'll need to pass in to our broker
	ctx := context.Background()
	b, err := broker.NewBroker(ctx, "postgres://abhirambanda:postgres@localhost:5434/distributed_job_queue")

	// Handle the broker creation error
	if err != nil{
		log.Printf("Failed to create broker: %v\n", err)
		return
	}

	// Creates a test job with payload and the idempotency key
	testJob1 := broker.Job{
		Payload: json.RawMessage(`{"type": "transcribe", "audio_url": "s3://recordings/visit-101.wav", "physician": "Dr. Smith"}`),
		IdempotencyKey: "transcribe-visit-101",
	}

	// Call the Submit function and handle potential error
	err = b.Submit(ctx, testJob1)
	if err != nil{
		log.Printf("Failed to submit the TestJob: %v\n", err)
		return
	}
}
