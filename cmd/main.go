package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"log"
	"fmt"
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

	// Testing Submit Method
	// Creates a test job with payload and the idempotency key
	testJob4 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-204", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-204",
	}

	// Call the Submit function and handle potential error
	err = b.Submit(ctx, testJob4)
	if err != nil{
		log.Printf("Failed to submit the TestJob: %v\n", err)
		return
	}

	// Testing Claim Method
	job, err := b.Claim(ctx)

	if err != nil{
		log.Printf("Failed to call Claim: %v\n", err)
		return
	}
	fmt.Printf("%+v\n", *job) 


	// //Testing Complete Method
	// uid := job.ID
	// err = b.Complete(ctx, uid)
	// if err != nil{
	// 	log.Printf("Failed to call Complete: %v\n", err)
	// }
	// log.Printf("Success")

	// Test the Fail Method
	uid := job.ID
	err = b.Fail(ctx, uid)
	if err != nil{
		log.Printf("Failed to call Fail: %v\n", err)
	}
	log.Printf("Failed once")

	err = b.Fail(ctx, uid)
	if err != nil{
		log.Printf("Failed to call Fail: %v\n", err)
	}
	log.Printf("Failed twice")

	err = b.Fail(ctx, uid)
	if err != nil{
		log.Printf("Failed to call Fail: %v\n", err)
	}
	log.Printf("Failed thrice")
}
