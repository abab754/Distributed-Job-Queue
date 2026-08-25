package main

import(
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"log"
	"fmt"
	"time"
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
	worker := broker.Worker{
		Broker: b,
		Handler: func(job broker.Job) error{
			fmt.Printf("Processing job: %s\n", job.ID)
			return fmt.Errorf("fake error")
		},
	}

	reaper := broker.Reaper{
		Broker: b,
	}

	// Testing Submit Method
	// Creates a test job with payload and the idempotency key
	testJob6 := broker.Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-206", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-206",
	}

	// Call the Submit function and handle potential error
	err = worker.Broker.Submit(ctx, testJob6)
	if err != nil{
		log.Printf("Failed to submit the TestJob: %v\n", err)
		return
	}

	job, err := worker.Broker.Claim(ctx)

	if err != nil{
		log.Printf("Failed to call Claim in Worker class: %v\n", err)
		return 
	}
	if job == nil{
		return
	}	

	go reaper.Begin(ctx)
	time.Sleep(45 * time.Second)
	//worker.Start(ctx)

	

	

	// // Testing Claim Method
	// job, err := b.Claim(ctx)

	// if err != nil{
	// 	log.Printf("Failed to call Claim: %v\n", err)
	// 	return
	// }
	// fmt.Printf("%+v\n", *job) 


	// // //Testing Complete Method
	// // uid := job.ID
	// // err = b.Complete(ctx, uid)
	// // if err != nil{
	// // 	log.Printf("Failed to call Complete: %v\n", err)
	// // }
	// // log.Printf("Success")

	// // Test the Fail Method
	// uid := job.ID
	// err = b.Fail(ctx, uid)
	// if err != nil{
	// 	log.Printf("Failed to call Fail: %v\n", err)
	// }
	// log.Printf("Failed once")

	// err = b.Fail(ctx, uid)
	// if err != nil{
	// 	log.Printf("Failed to call Fail: %v\n", err)
	// }
	// log.Printf("Failed twice")

	// err = b.Fail(ctx, uid)
	// if err != nil{
	// 	log.Printf("Failed to call Fail: %v\n", err)
	// }
	// log.Printf("Failed thrice")
}
