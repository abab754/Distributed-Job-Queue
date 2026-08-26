package main

import (
	"fmt"
	"log"
	"net/http"
	"github.com/abab754/Distributed-Job-Queue/internal"
	"context"
	"encoding/json"
	"os/signal"
	"syscall"
)


func main() {

	// Create a context that listens for SIGINT and SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() //cleans up resources when main exits

	fmt.Println("Application started. Press Ctrl+C to exit.")

	// Create a Broker by passing the context and connectionString
	b, err := broker.NewBroker(ctx, "postgres://abhirambanda:postgres@localhost:5434/distributed_job_queue")

	// Handle the broker creation error
	if err != nil{
		log.Printf("Failed to create broker: %v\n", err)
		return
	}

	// Create a new request multiplexer (router)
	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.Dir("./static")))

	// Register the handler function for the "/hello" route
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request){
		jobs, err := b.GetJobsByStatus(r.Context())
		if err != nil{
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobs)
	})

	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request){
		jobs, err := b.GetRecentJobs(r.Context())
		if err != nil{
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobs)
	})

	// Define the port the server will listen on
	port := ":8080"
	fmt.Printf("Server is running on http://localhost%s\n", port)

	server := http.Server{Addr: port, Handler: mux}

	go func(){
		<- ctx.Done()
		server.Shutdown(context.Background())
	}()
	err = server.ListenAndServe()
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}