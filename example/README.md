# Kafka Go Applications - Beginner's Guide

This repository contains beginner-friendly Go applications demonstrating how to work with Apache Kafka for message production and consumption.

## What is Apache Kafka?

Apache Kafka is a distributed streaming platform that allows you to:
- **Publish and subscribe** to streams of records (messages)
- **Store** streams of records in a fault-tolerant way
- **Process** streams of records as they occur

Think of Kafka like a **message highway** where:
- **Producers** are applications that send messages
- **Consumers** are applications that receive messages  
- **Topics** are like channels or categories for messages
- **Brokers** are Kafka servers that store and deliver messages

## Project Structure

```
.
├── docker-compose.yml     # Kafka cluster setup
├── go.mod                 # Go module dependencies
├── producer.go            # Go message producer
├── consumer.go            # Go message consumer
├── simple_producer.py     # Python message producer (recommended)
├── simple_consumer.py     # Python message consumer (recommended)
├── requirement.txt        # Python dependencies
└── README.md             # This file
```

## Prerequisites

Before running the applications, ensure you have:

1. **Docker & Docker Compose** installed
2. **Go** (version 1.19 or later) installed
3. **Git** for cloning repositories

## Quick Start

### Step 1: Start Kafka Cluster

First, start the Kafka cluster using Docker Compose:

```bash
# Start Kafka and Zookeeper in background
docker-compose up -d

# Check if services are running
docker-compose ps
```

You should see both `kafka` and `zookeeper` services running.

### Step 2: Install Dependencies

**For Go applications:**
```bash
# Download Go module dependencies
go mod tidy
```

**For Python applications:**
```bash
# Create virtual environment (if not exists)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/Mac
# or
venv\Scripts\activate     # On Windows

# Install Python dependencies
pip install -r requirement.txt
```

### Step 3: Create Kafka Topic (Optional)

Kafka will automatically create topics, but you can create one manually:

```bash
# Create a topic named 'my-topic'
docker exec -it kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List all topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Step 4: Run the Applications

Open **two terminal windows**:

#### Option A: Python Applications (Recommended for beginners)

**Terminal 1 - Start Consumer:**
```bash
# Activate virtual environment
source venv/bin/activate

# Run Python consumer
python simple_consumer.py
```

**Terminal 2 - Start Producer:**
```bash
# Activate virtual environment  
source venv/bin/activate

# Run Python producer
python simple_producer.py
```

#### Option B: Go Applications

**Terminal 1 - Start Consumer:**
```bash
go run consumer.go
```

**Terminal 2 - Start Producer:**
```bash
go run producer.go
```

You should see:
- Producer sending messages every 2 seconds
- Consumer receiving and displaying those messages

## Understanding the Code

### Python Applications (simple_producer.py / simple_consumer.py)

#### Python Producer
The producer creates and sends messages to Kafka using confluent-kafka:

```python
# Key configuration
config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'acks': 'all',                         # Wait for acknowledgment
    'retries': 3,                          # Retry failed sends
    'batch.size': 16384,                   # Batch size in bytes
}
producer = Producer(config)
```

#### Python Consumer
The consumer subscribes to topics and processes messages:

```python
# Key configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',       # Consumer group
    'auto.offset.reset': 'earliest',       # Start from beginning
    'enable.auto.commit': True,            # Auto commit offsets
}
consumer = Consumer(config)
```

### Go Applications (producer.go / consumer.go)

#### Go Producer
```go
// Key configuration
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092", // Kafka server address
    "acks":              "all",            // Wait for acknowledgment
    "retries":           "3",              // Retry failed sends
})
```

#### Go Consumer
```go
// Key configuration
consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers":  "localhost:9092",    
    "group.id":           "my-consumer-group", // Consumer group
    "auto.offset.reset":  "earliest",          // Start from beginning
})
```

**Key concepts:**
- **bootstrap.servers**: Address of Kafka brokers
- **acks**: Acknowledgment level (all = wait for all replicas)
- **retries**: How many times to retry failed sends
- **group.id**: Consumer group for scaling and fault tolerance
- **auto.offset.reset**: Where to start reading (earliest/latest)
- **enable.auto.commit**: Automatically track message processing

## Kafka Concepts Explained

### 1. Topics
- **What**: Named channels for organizing messages
- **Example**: `user-events`, `order-notifications`, `system-logs`
- **Analogy**: Like TV channels - consumers subscribe to specific topics

### 2. Partitions
- **What**: Topics are split into partitions for scalability
- **Purpose**: Allow parallel processing and increased throughput
- **Order**: Messages within a partition maintain order

### 3. Consumer Groups
- **What**: Group of consumers working together
- **Purpose**: Scale consumption and provide fault tolerance
- **Rule**: Each message goes to only one consumer in the group

### 4. Offsets
- **What**: Unique identifier for each message in a partition
- **Purpose**: Track which messages have been processed
- **Persistence**: Kafka remembers your progress

## Common Use Cases

1. **Microservices Communication**: Services communicate via Kafka topics
2. **Event Sourcing**: Store all changes as events
3. **Log Aggregation**: Collect logs from multiple services  
4. **Real-time Analytics**: Process data streams in real-time
5. **Notification Systems**: Send emails, SMS, push notifications

## Configuration Options

### Producer Settings
```go
&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "acks":              "all",     // 0, 1, or all
    "retries":           "3",       // Number of retries
    "batch.size":        "16384",   // Batch size in bytes
    "linger.ms":         "1",       // Wait time for batching
    "compression.type":  "snappy",  // Compression: none, gzip, snappy, lz4
}
```

### Consumer Settings
```go
&kafka.ConfigMap{
    "bootstrap.servers":       "localhost:9092",
    "group.id":                "my-group",
    "auto.offset.reset":       "earliest", // earliest, latest, none
    "enable.auto.commit":      "true",     // Auto commit offsets
    "auto.commit.interval.ms": "1000",     // Commit interval
    "session.timeout.ms":      "30000",    // Session timeout
}
```

## Testing the Applications

### Test Different Scenarios

1. **Multiple Consumers**: Run multiple consumer instances to see load balancing
   ```bash
   # Terminal 1
   go run consumer.go
   # Terminal 2  
   go run consumer.go
   # Terminal 3
   go run producer.go
   ```

2. **Different Consumer Groups**: Modify `group.id` to see message duplication
3. **Topic with Multiple Partitions**: Create topic with more partitions
4. **Error Handling**: Stop Kafka and see how applications handle failures

### Monitoring Commands

```bash
# Check consumer groups
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Check consumer group details
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-consumer-group

# Check topic details
docker exec -it kafka kafka-topics --describe --topic my-topic --bootstrap-server localhost:9092
```

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Check if Kafka is running: `docker-compose ps`
   - Verify port 9092 is accessible

2. **Topic Not Found**
   - Kafka creates topics automatically
   - Check topic exists: `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`

3. **No Messages Received**
   - Check producer is running and sending messages
   - Verify consumer group and topic names match

4. **Dependencies Issues**
   - Run `go mod tidy` to download dependencies
   - Ensure Go version is 1.19+

### Logs and Debugging

```bash
# Check Kafka logs
docker-compose logs kafka

# Check Zookeeper logs  
docker-compose logs zookeeper

# Run with verbose output
export CONFLUENT_KAFKA_GO_DEBUG_CONTEXT=1
go run producer.go
```

## Next Steps

Once comfortable with basics, explore:

1. **Advanced Configurations**: Serialization, schema registry
2. **Kafka Streams**: Stream processing applications
3. **Monitoring**: Prometheus, Grafana integration
4. **Production Setup**: Multi-broker clusters, security
5. **Different Clients**: Try other language clients (Python, Java)

## Cleanup

Stop and remove all containers:

```bash
# Stop services
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v

# Remove orphaned containers
docker-compose down --remove-orphans
```

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Go Client](https://github.com/confluentinc/confluent-kafka-go)
- [Kafka Concepts](https://kafka.apache.org/intro)

---

**Happy Messaging with Kafka! 🚀**
