# Distributed Job Queue

## Overview
This is a personal project to help me understand distributed systems from a low level. I built this Distributed Job Queue using Go and Postgres. Job Queues matter for many reasons including async processing and fault tolerance. It's especially relevant to systems in industries like healthcare where losing a job is unacceptable. 

## Architecture
The components that make up this Job Queue are a Broker, Workers, and a Reaper
Let's dive deeper into each one

### Broker
The Broker is where all the database operations live. It's also what exposes the methods via an API. 
The 4 methods that the Broker has are Submit, Claim, Complete, and Fail. These 4 functions are what the workers will need to call
in order to retrieve, write, and update data to the database.

Submit(): This method simply writes a job into the database. It accepts a Job struct and inserts its props into the db.

Claim(): This method is hefty. It first creates a transaction or a dedicated connection which won't return until explicitly told to do so. This is important becasue it'll ensure no other workers call this same task and will help lock the row we're getting. We then want to query the database for the oldest job whose status = 'pending'. If a job is returned, we store it and then we update the row's status to 'claimed'and set its lease to be the current time + 30 seconds. These two things ensure that other workers don't also claim this same task and the lease will help keep track of how much time a worker is taking for a task.

Complete(): This method is straightforward. It's a workers way of ACK'ing that it has completed a given job. This method takes in a jobId and updates the corresponding job in the database to status = 'completed'

Fail(): This is identical to Complete(). After the worker ACK's that it has failed its job, this method updates the corresponding row to be status='pending' again so that other workers can pick it up. However, we also store a MaxRetries variable. If a particular job fails more than a MaxRetries times, we set the status to be 'dead'. We keep track of the times a job has to be run by having an attempt_count field.

### Worker
Workers have an ID, a reference to a Broker, and a Handler function which is where the business logic for a task will live. 

A worker also has a Start method where the logic for a worker lives. This is a for loop which looks like this: 
1) check if the context is done(ctrl+c was called)
2) if it was called -> exit
3) else-> Claim a job
4) if no job available -> wait a second and retry
5) if job was found -> do said job(call handler)
6) if handler() errored -> call Fail() and continue
7) else -> Nice! call Complete() and continue

### Reaper
This is a background job which continuously checks for any jobs in the queue that stopped for unforseen reasons such as network drops, process kills, or OOM crashes. It catches jobs whose workers never ACK'd the result of it.  If a worker claims a job and doesn't ack the result before the lease ends, the reaper assumes the worker crashed and puts the job back in the queue for another worker to pick up.

## Design Decisions

### Why Postgres?
I used Postgres for this because it is easy to keep track of each job and its properties in the queue. It also provides locking rows which is crucial for when two workers try to claim at the same time. I also chose Postgres over redis or a message broker because postgres offers ACID transcations, data survives restarts without extra configs, and I can inspect the queue with plain SQL. Something like Redis would require extra work.

### Why `FOR UPDATE SKIP LOCKED`?
This is very important because it ensures no two workers claim the same task and complete it leading to duplicate results. For example if worker1 and worker2 both call Claim() trying to look for a task within 0.5 ms of each other, they could possible both claim the same task. FOR UPDATE SKIP LOCKED prevents this by locking the row after worker1 calls Claim(). Worker2 will then call Claim() and get a separate job since it skipped the locked one which worker1 picked up. 

### Why At-Least-Once Delivery?
Ensures that no data is ever lost even though we may see duplicates. If an acknowledgement is missing for any reason, the sender sends a message again. Why not exactly-once? True exactly-once is impossible in distributed systems — you can't guarantee the worker finished and the ack was received atomically. So you pick at-least-once and handle duplicates with idempotency keys. I have an idempotency key in a job's properties to ensure if a duplicate job is found, we can easily check and prevent duplicate processing. 

### Why Leases Over Heartbeats?
Heartbeats require the worker to actively send signals, which means the broker needs to track heartbeat state per worker. Leases are passive — time itself is the failure detector. No extra protocol is needed

## How to Run

### Prerequisites
Install go

### Setup
```bash
git clone <repo-url>

docker-compose up -d 

# Create a .env file with POSTGRES_USER and POSTGRES_PASSWORD

create .env file 

psql -U <user> -h localhost -p 5434 -d distributed_job_queue -f migrations/001_create_jobs_table.sql

go run cmd/main.go
```

### Running Workers
Create workers in main.go and run go routines for each while calling Start()

## How to Test
```bash
go test ./internal/ -v.
```
## What I'd Do With More Time
- Priority queues — let producers assign
  priority levels so urgent jobs get claimed
  first
- Rate limiting — control how many jobs a
worker can process per minute
- Retry backoff — instead of immediately
retrying failed jobs, wait with exponential
delay (1s, 2s, 4s...) to avoid hammering a
failing downstream service
- Metrics/observability — track throughput,
latency, failure rates (Prometheus/Grafana)
- Transactional outbox pattern — for
closer-to-exactly-once semantics