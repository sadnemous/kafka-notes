#!/usr/bin/env python3
"""
Simple Kafka Producer in Python using confluent-kafka

This script sends messages to a Kafka topic every 2 seconds.
Press Ctrl+C to stop gracefully.
"""

import json
import signal
import sys
import time
from datetime import datetime

# Try importing confluent-kafka (more reliable)
try:
    from confluent_kafka import Producer
    print("Using confluent-kafka library")
except ImportError:
    print("❌ confluent-kafka library not installed!")
    print("Please install with: pip install confluent-kafka")
    sys.exit(1)


class SimpleKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'my-topic'
        self.message_count = 0
        self.running = True
        
        # Kafka producer configuration
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 1,
        }
        
        self.producer = Producer(self.config)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        """Handle shutdown signals"""
        print("\n⚠️  Shutting down producer...")
        self.running = False
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f"❌ Message delivery failed: {err}")
        else:
            print(f"✓ Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
    
    def send_message(self, message, key=None):
        """Send a message to Kafka"""
        try:
            # Send message asynchronously
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=message,
                callback=self.delivery_callback
            )
            
            # Trigger delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
    
    def run(self):
        """Main producer loop"""
        print("🚀 Starting Simple Kafka Producer...")
        print(f"📡 Connected to: {self.bootstrap_servers}")
        print(f"📋 Topic: {self.topic}")
        print("Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                self.message_count += 1
                
                # Create message
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                message = f"Hello from Simple Python Producer! Message #{self.message_count} - {timestamp}"
                key = f"key-{self.message_count}"
                
                # Send message
                print(f"📤 Sending: {message}")
                self.send_message(message, key)
                
                # Wait 2 seconds
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n⚠️  Keyboard interrupt received")
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        print("🔄 Flushing remaining messages...")
        self.producer.flush(15)  # Wait up to 15 seconds
        print(f"📊 Total messages sent: {self.message_count}")
        print("👋 Goodbye!")


if __name__ == "__main__":
    print("=" * 60)
    print("        SIMPLE KAFKA PYTHON PRODUCER")
    print("=" * 60)
    
    producer = SimpleKafkaProducer()
    producer.run()
