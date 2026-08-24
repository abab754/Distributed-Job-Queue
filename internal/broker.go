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
	return nil, nil
}

// Worker Acknowledges the fact that it successfully completed its task
func (b *Broker) Complete(ctx context.Context, jobId uuid.UUID) (error){
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

