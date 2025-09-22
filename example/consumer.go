package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Create a new consumer instance
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",    // Kafka broker address
		"group.id":           "my-consumer-group", // Consumer group ID
		"auto.offset.reset":  "earliest",          // Start from beginning if no offset
		"enable.auto.commit": "true",              // Automatically commit offsets
		"auto.commit.interval.ms": "1000",         // Commit interval in milliseconds
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	// Close consumer when function ends
	defer consumer.Close()

	// Topic to subscribe to
	topic := "my-topic"

	// Subscribe to topic
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic %s: %s", topic, err)
	}

	// Handle Ctrl+C gracefully
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Kafka Consumer started. Press Ctrl+C to stop.")
	fmt.Printf("Subscribed to topic: %s\n", topic)
	fmt.Printf("Consumer group: my-consumer-group\n")
	fmt.Println("Waiting for messages...")
	fmt.Println("---")

	// Message counter
	messageCount := 0

	// Main consumption loop
	for {
		select {
		case <-signals:
			fmt.Println("\n⚠️  Shutting down consumer...")
			fmt.Printf("Total messages received: %d\n", messageCount)
			fmt.Println("Consumer stopped.")
			return

		default:
			// Poll for messages with timeout
			message, err := consumer.ReadMessage(100) // 100ms timeout

			if err != nil {
				// Timeout is expected, continue
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v", err)
				continue
			}

			// Process the message
			messageCount++
			
			fmt.Printf("📨 Message #%d received:\n", messageCount)
			fmt.Printf("   Topic: %s\n", *message.TopicPartition.Topic)
			fmt.Printf("   Partition: %d\n", message.TopicPartition.Partition)
			fmt.Printf("   Offset: %d\n", message.TopicPartition.Offset)
			fmt.Printf("   Key: %s\n", string(message.Key))
			fmt.Printf("   Value: %s\n", string(message.Value))
			
			// Check if message has timestamp
			if !message.Timestamp.IsZero() {
				fmt.Printf("   Timestamp: %s\n", message.Timestamp.Format("2006-01-02 15:04:05"))
			}
			fmt.Println("---")

			// Optional: Manual commit (if auto commit is disabled)
			// consumer.CommitMessage(message)
		}
	}
}
