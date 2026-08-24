package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	// "encoding/json"
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

	// Creates a test job with payload and the idempotency key
	// testJob2 := broker.Job{
	// 	Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-202", "template": "soap_note"}`),
	// 	IdempotencyKey: "note-visit-202",
	// }

	// Call the Submit function and handle potential error
	// err = b.Submit(ctx, testJob2)
	// if err != nil{
	// 	log.Printf("Failed to submit the TestJob: %v\n", err)
	// 	return
	// }

	job, err := b.Claim(ctx)

	if err != nil{
		log.Printf("Failed to call Claim: %v\n", err)
		return
	}
	fmt.Printf("%+v\n", *job) 

}
