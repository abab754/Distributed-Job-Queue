package broker

import (
	"testing"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

func setup(t *testing.T) (*Broker, context.Context){
	ctx := context.Background()
	b, err := NewBroker(ctx, "postgres://abhirambanda:postgres@localhost:5434/distributed_job_queue")

	if err != nil{
		t.Fatalf("Failed to create broker: %v\n", err)
	}

	b.Pool.Exec(ctx, "DELETE FROM jobs")
	return b, ctx
}

func TestSubmitAndClaim(t *testing.T){
	b, ctx := setup(t)

	testJob1 := Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err := b.Submit(ctx, testJob1)
	if err != nil{
		t.Fatalf("Failed to submit the TestJob1: %v\n", err)
	}
	fmt.Printf("Job was successfully submitted\n")

	job, err := b.Claim(ctx)
	if err != nil{
		t.Fatalf("Failed to call Claim in Worker class: %v\n", err)
	}
	
	// If no Job was returned: wait a second and continue
	if job == nil{
		t.Fatalf("No jobs are available at the moment\n")
	}
	fmt.Printf("Job was successfully claimed\n")

	if job.IdempotencyKey != "note-visit-101" {
		t.Errorf("expected idempotency key 'note-visit-101', got '%s'", job.IdempotencyKey)
	}
}

func TestClaimEmptyQueue(t *testing.T){
	b, ctx := setup(t)

	job, err := b.Claim(ctx)

	if job != nil || err != nil{
		t.Errorf("expected job and error to be nil becasue of empty queue")
	}
}

func TestComplete(t *testing.T){
	b, ctx := setup(t)

	testJob1 := Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err := b.Submit(ctx, testJob1)
	if err != nil{
		t.Fatalf("Failed to submit the TestJob1: %v\n", err)
	}
	fmt.Printf("Job was successfully submitted\n")

	job, err := b.Claim(ctx)
	if err != nil{
		t.Fatalf("Failed to call Claim: %v\n", err)
	}
	
	// If no Job was returned: wait a second and continue
	if job == nil{
		t.Fatalf("No jobs are available at the moment\n")
	}
	fmt.Printf("Job was successfully claimed\n")

	err = b.Complete(ctx, job.ID)
	if err != nil{
		t.Fatalf("Failed to complete task: %v\n", err)
	}

	query := `
			SELECT status FROM jobs 
			WHERE job_id = $1
			LIMIT 1
		`

	var j Job
	err = b.Pool.QueryRow(ctx, query, job.ID).Scan(&j.Status)

	if j.Status != "completed"{
		t.Errorf("expected job's status to be 'completed'.")
	}
}

func TestFailAndRetry(t *testing.T){
	b, ctx := setup(t)

	testJob1 := Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err := b.Submit(ctx, testJob1)
	if err != nil{
		t.Fatalf("Failed to submit the TestJob1: %v\n", err)
	}
	fmt.Printf("Job was successfully submitted\n")

	job, err := b.Claim(ctx)
	if err != nil{
		t.Fatalf("Failed to call Claim: %v\n", err)
	}
	
	// If no Job was returned: wait a second and continue
	if job == nil{
		t.Fatalf("No jobs are available at the moment\n")
	}
	fmt.Printf("Job was successfully claimed\n")

	err = b.Fail(ctx, job.ID)
	if err != nil{
		t.Fatalf("Failed to call Fail(): %v\n", err)
	}

	query := `
			SELECT status, attempt_count FROM jobs 
			WHERE job_id = $1
			LIMIT 1
		`

	var j Job
	err = b.Pool.QueryRow(ctx, query, job.ID).Scan(&j.Status, &j.AttemptCount)

	if j.Status != "pending" || j.AttemptCount != 1{
		t.Errorf("expected job's status to be 'pending' and the AttemptCount to be 1.")
	}
}

func TestFailToDeadLetter(t *testing.T){
	b, ctx := setup(t)

	testJob1 := Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err := b.Submit(ctx, testJob1)
	if err != nil{
		t.Fatalf("Failed to submit the TestJob1: %v\n", err)
	}
	fmt.Printf("Job was successfully submitted\n")
	
	job, err := b.Claim(ctx)
	if err != nil{
		t.Fatalf("Failed to call Claim: %v\n", err)
	}
	
	// If no Job was returned: wait a second and continue
	if job == nil{
		t.Fatalf("No jobs are available at the moment\n")
	}
	fmt.Printf("Job was successfully claimed\n")

	maxRetries := 3
	jobId := job.ID

	for i:=0; i < maxRetries; i++{
		err = b.Fail(ctx, job.ID)
		if err != nil{
			t.Fatalf("Failed to call Fail(): %v\n", err)
		}
		
		if i < maxRetries - 1{
			job, err = b.Claim(ctx)
			if err != nil{
				t.Fatalf("Failed to call Claim: %v\n", err)
			}
		}
	}

	query := `
			SELECT status FROM jobs 
			WHERE job_id = $1
			LIMIT 1
		`

	var j Job
	err = b.Pool.QueryRow(ctx, query, jobId).Scan(&j.Status)

	if j.Status != "dead"{
		t.Errorf("expected job's status to be 'dead'")
	}
}

func TestNoDuplicateClaims(t *testing.T){
	b, ctx := setup(t)

	testJob1 := Job{
		Payload: json.RawMessage(`{"type": "generate_note", "visit_id": "visit-101", "template": "soap_note"}`),
		IdempotencyKey: "note-visit-101",
	}

	// Call the Submit function and handle potential error
	err := b.Submit(ctx, testJob1)
	if err != nil{
		t.Fatalf("Failed to submit the TestJob1: %v\n", err)
	}
	fmt.Printf("Job was successfully submitted\n")

	var wg sync.WaitGroup

	results := make(chan *Job, 2)
	wg.Add(2)

	go func(){
		defer wg.Done()
		job, _ := b.Claim(ctx)
		results <- job
	}()
	go func(){
		defer wg.Done()
		job, _ := b.Claim(ctx)
		results <- job
	}()

	wg.Wait()
	close(results)

	count:= 0
	for j := range results{
		if j != nil{
			count++
		}
	}
	if count != 1{
		t.Errorf("expected exactly one job to be non nil")
	}
}