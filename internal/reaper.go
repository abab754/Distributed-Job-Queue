package broker

import (
	"fmt"
	"context"
	"log"
	"time"
)

// Reaper struct- contains a reference to a Broker
type Reaper struct {
	// Reference to a Broker struct
	Broker *Broker
}


func (reaper *Reaper) Begin(ctx context.Context) error{
	// Create a ticker that delivers ticks every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	
	// Ensure the ticker is cleaned up when the function returns
	defer ticker.Stop()

	fmt.Println("Loop started...")

	// Loop will run every time a new value is received from ticker.C
	for{
		select{
			case <- ctx.Done():
				log.Printf("Reaper is shutting down")
				return nil
			case t := <-ticker.C:
				fmt.Printf("Tick action triggered at: %v\n", t.Format("15:04:05"))
			
				// Insert your recurring logic here
				query := `UPDATE jobs SET status = 'pending', lease = NULL WHERE status = 'claimed' AND lease < NOW()` 

				tag, err := reaper.Broker.Pool.Exec(ctx, query)
				if err != nil {
					log.Printf("Failed to execute query: %v\n", err)
					return err
				}
				if tag.RowsAffected() == 0 {
					fmt.Printf("no jobs were reaped")
				}
				log.Printf("Updated and Returned %d rows \n", tag.RowsAffected())

		}
	}
	return nil
}
