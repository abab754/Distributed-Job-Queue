package broker

import (
	"context"
)

// New struct to hold the status of a job and the number of those jobs
type JobTypeCount struct{
	Status string `json:"status"`
	Count int `json:"count"`
}

// Queries the Database and returns the number of jobs for each status type
// For the dashboard
func (b *Broker) GetJobsByStatus(ctx context.Context) ([]JobTypeCount, error){
	rows, err := b.Pool.Query(ctx, "SELECT status, COUNT(*) as job_count FROM jobs GROUP BY status")

	if err != nil{
		return nil, err
	}

	defer rows.Close()

	var jobs []JobTypeCount
	for rows.Next(){
		var j JobTypeCount
		if err := rows.Scan(&j.Status, &j.Count); err != nil{
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Queries the Database for the 20 most recently created jobs
func (b *Broker) GetRecentJobs(ctx context.Context) ([]Job, error){
	rows, err := b.Pool.Query(ctx, "SELECT job_id, status, attempt_count, created_at FROM jobs ORDER BY created_at DESC LIMIT 20")

	if err != nil{
		return nil, err
	}

	defer rows.Close()

	var jobs []Job

	for rows.Next(){
		var j Job
		if err := rows.Scan(&j.ID, &j.Status, &j.AttemptCount, &j.CreatedAt); err != nil{
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
