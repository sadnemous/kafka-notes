#!/usr/bin/env python3
"""
Multi-Partition Kafka Producer in Python

This producer demonstrates:
1. Creating topics with multiple partitions
2. Controlling message partition assignment
3. Using different partitioning strategies
"""

import json
import signal
import sys
import time
from datetime import datetime
import hashlib

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    print("Using confluent-kafka library")
except ImportError:
    print("❌ confluent-kafka library not installed!")
    print("Please install with: pip install confluent-kafka")
    sys.exit(1)


class MultiPartitionProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'multi-partition-topic'
        self.num_partitions = 3
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
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        
        # Create topic with multiple partitions
        self.create_topic()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def create_topic(self):
        """Create topic with multiple partitions if it doesn't exist"""
        try:
            # Check if topic exists
            topic_metadata = self.admin_client.list_topics(timeout=10)
            
            if self.topic in topic_metadata.topics:
                existing_partitions = len(topic_metadata.topics[self.topic].partitions)
                print(f"✓ Topic '{self.topic}' already exists with {existing_partitions} partitions")
                return
            
            # Create new topic
            topic_list = [NewTopic(
                topic=self.topic, 
                num_partitions=self.num_partitions, 
                replication_factor=1
            )]
            
            print(f"🔧 Creating topic '{self.topic}' with {self.num_partitions} partitions...")
            
            fs = self.admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print(f"✓ Topic '{topic}' created successfully!")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"✓ Topic '{topic}' already exists")
                    else:
                        print(f"❌ Failed to create topic '{topic}': {e}")
                        
        except Exception as e:
            print(f"⚠️  Could not create topic: {e}")
            print("   Topic will be auto-created with default settings")
    
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
    
    def send_message_round_robin(self, message, key=None):
        """Send message using round-robin partitioning (None key)"""
        try:
            self.producer.produce(
                topic=self.topic,
                key=None,  # No key = round-robin
                value=message,
                callback=self.delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
    
    def send_message_keyed(self, message, key):
        """Send message using key-based partitioning"""
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,  # Same key always goes to same partition
                value=message,
                callback=self.delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
    
    def send_message_specific_partition(self, message, partition, key=None):
        """Send message to specific partition"""
        try:
            self.producer.produce(
                topic=self.topic,
                partition=partition,  # Specify exact partition
                key=key,
                value=message,
                callback=self.delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
    
    def run(self):
        """Main producer loop demonstrating different partitioning strategies"""
        print("🚀 Starting Multi-Partition Kafka Producer...")
        print(f"📡 Connected to: {self.bootstrap_servers}")
        print(f"📋 Topic: {self.topic}")
        print(f"🔢 Partitions: {self.num_partitions}")
        print("Press Ctrl+C to stop\n")
        
        strategies = [
            ("Round-Robin", "round_robin"),
            ("Key-Based", "keyed"),
            ("Specific Partition", "specific")
        ]
        
        strategy_index = 0
        
        try:
            while self.running:
                self.message_count += 1
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Cycle through different strategies
                strategy_name, strategy_type = strategies[strategy_index % len(strategies)]
                
                if strategy_type == "round_robin":
                    message = f"[ROUND-ROBIN] Message #{self.message_count} - {timestamp}"
                    print(f"📤 Sending (Round-Robin): {message}")
                    self.send_message_round_robin(message)
                    
                elif strategy_type == "keyed":
                    # Use user IDs as keys - same user always goes to same partition
                    user_id = f"user-{(self.message_count % 5) + 1}"  # user-1 through user-5
                    key = user_id.encode('utf-8')
                    message = f"[KEY-BASED] Message from {user_id} #{self.message_count} - {timestamp}"
                    print(f"📤 Sending (Key={user_id}): {message}")
                    self.send_message_keyed(message, key)
                    
                elif strategy_type == "specific":
                    partition = self.message_count % self.num_partitions
                    message = f"[SPECIFIC-PARTITION] Message #{self.message_count} - {timestamp}"
                    print(f"📤 Sending (Partition {partition}): {message}")
                    self.send_message_specific_partition(message, partition)
                
                strategy_index += 1
                
                # Wait 6 seconds
                time.sleep(6)
                
        except KeyboardInterrupt:
            print("\n⚠️  Keyboard interrupt received")
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources"""
        print("🔄 Flushing remaining messages...")
        self.producer.flush(15)
        print(f"📊 Total messages sent: {self.message_count}")
        print("👋 Goodbye!")


if __name__ == "__main__":
    print("=" * 70)
    print("        MULTI-PARTITION KAFKA PRODUCER")
    print("=" * 70)
    
    producer = MultiPartitionProducer()
    producer.run()
