/*
	This is where all the database operations live
	The workers will call these functions
*/
package broker

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/google/uuid"
	"encoding/json"
	"time"
	"log"
	"errors"
	"github.com/jackc/pgx/v5"
)

type Broker struct {
	// Connection to Postgres db
	Pool *pgxpool.Pool
}

type Job struct{
	ID uuid.UUID
	Status string
	Payload json.RawMessage
	Lease time.Time
	CreatedAt time.Time
	IdempotencyKey string
	AttemptCount int
}

//Adds a job to the Queue(Database)
func (b *Broker) Submit(ctx context.Context, job Job) error{
	// Generate a new uuid for the jobID
	idV4 := uuid.New()
	job.ID = idV4

	// Execute the Query - Only the fields provided, not the default fields
	insertQuery := `INSERT INTO jobs (job_id, payload, idempotency_key)
	 VALUES ($1, $2, $3);`

	// Execute the query with the broker's Pool and handle error if needed
	_, err := b.Pool.Exec(ctx, insertQuery, job.ID, job.Payload, job.IdempotencyKey)
	if err != nil {
		log.Printf("Failed to insert job: %v\n", err)
		return err
	}

	fmt.Println("Successfully inserted job!")
	return nil
}

// Worker claims the next available job from the queue
func (b *Broker) Claim(ctx context.Context) (*Job, error){
	// Creates a dedicated connection and wont return until explicitly told to do so
	tx, err := b.Pool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to start transaction: %v", err)
		return nil, err
	}
	// defer is used to call Rollback in case the function crashes before commit is called
	defer tx.Rollback(ctx)

	// Since we want the oldest pending job, we query suing limit 1
	query := `SELECT job_id, status, payload, attempt_count, created_at, idempotency_key FROM jobs 
			WHERE status = 'pending'
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED`

	// Create a Job variable which we will use to store our result in and return it
	var job Job

	// Execute the Query and store the results in job variable using Scan()
	err = tx.QueryRow(ctx, query).Scan(&job.ID, &job.Status, &job.Payload, &job.AttemptCount, &job.CreatedAt, &job.IdempotencyKey)
	if err != nil {
		// Handle the empty queue safely instead of crashing
		if errors.Is(err, pgx.ErrNoRows) {
			log.Println("No pending jobs found. Queue is empty.")
			return nil, nil
		}

		// Handle when there is an actual error with the query
		log.Printf("Failed to lock job: %v", err)
		return nil, err
	}
	log.Printf("Successfully locked and retrieved Job ID: %s\n", job.ID)

	// Perform atomic operations
	// We want to update the lease to keep track if a particular job is taking too long to execute
	_, err = tx.Exec(ctx, "UPDATE jobs SET lease = NOW() + INTERVAL '30 seconds', status = 'claimed' WHERE job_id = $1", job.ID)
	if err != nil {
		return nil, err
	}

	// Safely end the transaction
	err = tx.Commit(ctx)
	return &job, err
}

// Worker Acknowledges the fact that it successfully completed its task
func (b *Broker) Complete(ctx context.Context, jobId uuid.UUID) (error){
	query := `UPDATE jobs SET status = 'completed', lease = NULL WHERE job_id = $1`

	tag, err := b.Pool.Exec(ctx, query, jobId)
	if err != nil {
		log.Printf("Failed to lock job: %v", err)
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no job found with id %s", jobId)
	}
	log.Printf("Completed job with job_id of: %s\n", jobId)
	return nil
}

// Worker acknowledges that it failed the task
func (b *Broker) Fail(ctx context.Context, jobId uuid.UUID) (error){
	return nil
}

// NewClient initializes and pings a new connection pool.
func NewBroker(ctx context.Context, connString string) (*Broker, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &Broker{Pool: pool}, nil
}

// Close safely shuts down the connection pool.
func (b *Broker) Close() {
	b.Pool.Close()
}

