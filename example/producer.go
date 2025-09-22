package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Create a new producer instance
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092", // Kafka broker address
		"acks":              "all",            // Wait for all replicas to acknowledge
		"retries":           "3",              // Number of retries on failure
		"batch.size":        "16384",          // Batch size in bytes
		"linger.ms":         "1",              // Wait time before sending batch
	})

	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	// Close producer when function ends
	defer producer.Close()

	// Topic to send messages to
	topic := "my-topic"

	// Handle Ctrl+C gracefully
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Counter for message numbering
	messageCount := 1

	fmt.Println("Kafka Producer started. Press Ctrl+C to stop.")
	fmt.Printf("Sending messages to topic: %s\n", topic)

	// Create a ticker to send messages every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Main loop
	for {
		select {
		case <-ticker.C:
			// Create message
			message := fmt.Sprintf("Hello from Go Producer! Message #%d - %s", 
				messageCount, time.Now().Format("2006-01-02 15:04:05"))
			
			// Produce message to topic
			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("key-%d", messageCount)),
				Value:          []byte(message),
			}, nil)

			if err != nil {
				log.Printf("Failed to produce message: %s", err)
			} else {
				fmt.Printf("✓ Sent: %s\n", message)
			}

			messageCount++

		case <-signals:
			fmt.Println("\n⚠️  Shutting down producer...")
			// Flush any remaining messages
			producer.Flush(15 * 1000) // Wait up to 15 seconds
			fmt.Println("Producer stopped.")
			return
		}

		// Handle delivery reports (optional)
		go func() {
			for e := range producer.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Printf("❌ Delivery failed: %v", ev.TopicPartition.Error)
					}
				}
			}
		}()
	}
}
