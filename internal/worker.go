package broker

import (
	"fmt"
	"context"
	"log"
	"time"
)

// Worker struct- contains a reference to a Broker and a Handler function which will hold business logic
type Worker struct {
	// Reference to a Broker struct
	Broker *Broker
	Handler func(job Job) error
}

/* 
	Method to start the worker loop:
	Claim() a task
	if no task -> timeout and continue
	if task -> call the handler and grab result
	if handler() errored -> call Fail()
	else -> call Complete()

*/
func (worker *Worker) Start(ctx context.Context) error{

	for {
		// Testing Claim Method
		job, err := worker.Broker.Claim(ctx)

		if err != nil{
			log.Printf("Failed to call Claim in Worker class: %v\n", err)
			return err
		}
		
		// If no Job was returned: wait a second and continue
		if job == nil{
			fmt.Printf("No jobs are available at the moment")
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("Successfully called Claim in Worker class\n%+v\n", *job) 

		// Call Handler
		err = worker.Handler(*job)

		// CHeck if the Handler executed the task properly
		if err != nil{
			log.Printf("Failed to call Handler in Worker class: %v\n", err)
			log.Printf("Calling Fail() \n")
			err = worker.Broker.Fail(ctx, job.ID)
			if err != nil{
				log.Printf("Something went wrong in the Fail method: %v\n", err)
			}
			log.Printf("Successfully called Fail()")
			continue
		}

		// If the task wasn't finished properly -> call fail
		err = worker.Broker.Complete(ctx, job.ID)
		if err != nil{
			log.Printf("Something went wrong in the Complete method: %v\n", err)
		}
		log.Printf("Successfully called Complete()")
	}
}
