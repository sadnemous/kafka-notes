#!/usr/bin/env python3
"""
Kafka Consumer #2 - Consumer Group: consumer-group-2

This consumer uses group ID 'consumer-group-2' and will track its own offsets.
This consumer will receive ALL messages (separate from consumer-group-1).
"""

import signal
import sys
from datetime import datetime

# Try importing confluent-kafka (more reliable)
try:
    from confluent_kafka import Consumer, KafkaError
    print("Using confluent-kafka library")
except ImportError:
    print("❌ confluent-kafka library not installed!")
    print("Please install with: pip install confluent-kafka")
    sys.exit(1)


class KafkaConsumer2:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'my-topic'
        self.group_id = 'consumer-group-2'  # Different group ID
        self.message_count = 0
        self.running = True
        
        # Kafka consumer configuration
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
        }
        
        self.consumer = Consumer(self.config)
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n⚠️  Shutting down consumer...")
        self.running = False
    
    def process_message(self, msg):
        """Process a received message"""
        self.message_count += 1
        
        print(f"\n📨 Message #{self.message_count} received:")
        print(f"   Topic: {msg.topic()}")
        print(f"   Partition: {msg.partition()}")
        print(f"   Offset: {msg.offset()}")
        
        # Display key
        key = msg.key()
        if key:
            print(f"   Key: {key.decode('utf-8')}")
        else:
            print("   Key: None")
        
        # Display value
        value = msg.value()
        if value:
            print(f"   Value: {value.decode('utf-8')}")
        else:
            print("   Value: None")
        
        # Display timestamp
        timestamp = msg.timestamp()
        if timestamp[1] != -1:  # timestamp[0] is type, timestamp[1] is value
            ts_datetime = datetime.fromtimestamp(timestamp[1] / 1000.0)
            print(f"   Timestamp: {ts_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("   " + "-" * 50)
    
    def run(self):
        """Main consumer loop"""
        print("🚀 Starting Kafka Consumer #2...")
        print(f"📡 Connected to: {self.bootstrap_servers}")
        print(f"📋 Subscribed to topic: {self.topic}")
        print(f"👥 Consumer group: {self.group_id}")
        print("Waiting for messages...\n")
        
        try:
            while self.running:
                # Poll for messages (1 second timeout)
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message received within timeout
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        continue
                    else:
                        print(f"❌ Consumer error: {msg.error()}")
                        break
                else:
                    # Process the message
                    self.process_message(msg)
        
        except KeyboardInterrupt:
            print(f"\n⚠️  Keyboard interrupt received")
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        print("🔄 Closing consumer...")
        self.consumer.close()
        print(f"📊 Total messages processed: {self.message_count}")
        print("👋 Consumer #2 says goodbye!")


if __name__ == "__main__":
    print("=" * 60)
    print("        KAFKA CONSUMER #2 (Group: consumer-group-2)")
    print("=" * 60)
    
    consumer = KafkaConsumer2()
    consumer.run()
