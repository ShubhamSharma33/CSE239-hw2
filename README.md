# Dockerized MapReduce Word Count System

## Overview
This project implements a distributed MapReduce system using Docker containers and RPyC (Remote Python Calls) for word counting on large text datasets. The system consists of one coordinator container and multiple worker containers that communicate via Docker networking.

## Architecture
```
     +----------------------+
     |    COORDINATOR       | ← Docker Container (coordinator)
     +----------------------+
            |   |   |   |
    map() → |   |   |   | ← reduce() [Docker networking]
            ↓   ↓   ↓   ↓
    +----------+ +----------+ +----------+ 
    | WORKER 1 | | WORKER 2 | | ...      | ← Docker Containers
    +----------+ +----------+ +----------+
```

## Prerequisites
- Docker Engine (20.10+)
- Docker Compose (2.0+)
- Python 3.9+
- Git

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/mapreduce-docker.git
cd mapreduce-docker
```

### 2. Project Structure
```
mapreduce-docker/
├── coordinator.py      # Coordinator implementation
├── worker.py          # Worker implementation  
├── Dockerfile         # Docker image definition
├── docker-compose.yml # Multi-container orchestration
├── requirements.txt   # Python dependencies
└── README.md         # This file
```

### 3. Build Docker Images
```bash
docker-compose build
```

### 4. Run with Different Worker Configurations

**Run with 1 worker:**
```bash
docker-compose up --scale worker=1
```

**Run with 2 workers:**
```bash
docker-compose up --scale worker=2
```

**Run with 4 workers:**
```bash
docker-compose up --scale worker=4
```

**Run with 8 workers:**
```bash
docker-compose up --scale worker=8
```

### 5. Stop the System
```bash
docker-compose down
```

## File Descriptions

### coordinator.py
The coordinator manages the entire MapReduce workflow:
- Downloads and prepares the dataset
- Distributes map tasks to workers
- Handles worker failures with 20-second timeout
- Performs shuffle phase (grouping by keys)
- Distributes reduce tasks
- Aggregates final results
- Reports execution time

### worker.py
```python
import rpyc

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """Map step: tokenize and count words in text chunk."""
        word_count = {}
        for word in text_chunk.lower().split():
            word = ''.join(c for c in word if c.isalnum())
            if word:
                word_count[word] = word_count.get(word, 0) + 1
        return word_count
    
    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""
        result = {}
        for word, counts in grouped_items.items():
            result[word] = sum(counts)
        return result

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MapReduceService, port=18865)
    print("[Worker] Starting RPyC service on port 18865...")
    t.start()
```

### Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir rpyc requests

# Copy application files
COPY coordinator.py worker.py ./

# Default to worker mode
CMD ["python", "worker.py"]
```

### docker-compose.yml
```yaml
version: '3.8'

services:
  coordinator:
    build: .
    container_name: coordinator
    command: python coordinator.py
    environment:
      - DATA_URL=https://mattmahoney.net/dc/enwik9.zip
    depends_on:
      - worker
    networks:
      - mapreduce-net
    volumes:
      - ./txt:/app/txt

  worker:
    build: .
    networks:
      - mapreduce-net
    deploy:
      replicas: 3

networks:
  mapreduce-net:
    driver: bridge
```

### requirements.txt
```
rpyc==5.3.1
requests==2.31.0
```

## Key Features

### 1. Fault Tolerance
- Workers are monitored with 20-second timeout
- Failed tasks are automatically reassigned to available workers
- Retry logic with multiple attempts before marking task as failed

### 2. Parallel Processing
- Map tasks distributed across all available workers
- Concurrent execution using ThreadPoolExecutor
- Dynamic load balancing with round-robin + retry

### 3. Docker Networking
- Workers communicate via Docker DNS (hostnames: worker-1, worker-2, etc.)
- Port 18865 for RPyC communication
- Bridge network for container isolation

### 4. Scalability
- Supports 1-8 worker containers
- Chunks dynamically adjusted based on worker count
- Hash-based partitioning for balanced reduce phase

## Configuration

### Environment Variables
- `DATA_URL`: URL to download dataset (default: enwik9.zip)

### Network Configuration
- Workers listen on port 18865
- Coordinator connects to workers via Docker DNS
- Network name: `mapreduce-net`

## Testing with Different Datasets

**Test with smaller dataset (enwik8):**
```bash
docker-compose run coordinator python coordinator.py
# Modify DATA_URL in docker-compose.yml to use enwik8.zip
```

**Test with custom dataset:**
```bash
docker-compose run -e DATA_URL=http://your-dataset-url.zip coordinator python coordinator.py
```

## Performance Metrics
The system reports:
- Total execution time
- Top 20 most frequent words
- Individual phase completion times (Map, Shuffle, Reduce)

## Troubleshooting

**Workers not connecting:**
```bash
# Check worker logs
docker-compose logs worker

# Verify network connectivity
docker network inspect mapreduce-docker_mapreduce-net
```

**Out of memory errors:**
```bash
# Increase Docker memory limit
docker-compose --compatibility up
```

**Port conflicts:**
```bash
# Check if port 18865 is in use
netstat -an | grep 18865
```

## Clean Up
```bash
# Remove all containers and networks
docker-compose down -v

# Remove downloaded datasets
rm -rf txt/

# Remove Docker images
docker-compose down --rmi all
```

## Implementation Details

### MapReduce Workflow
1. **Initialization**: Coordinator downloads dataset, establishes worker connections
2. **Map Phase**: Text split into chunks, distributed to workers for word counting
3. **Shuffle Phase**: Intermediate results grouped by word key
4. **Reduce Phase**: Grouped data partitioned and sent to workers for aggregation
5. **Final Aggregation**: Results combined and sorted by frequency

### Mutual Exclusion
- Thread-safe operations using ThreadPoolExecutor
- RPyC handles concurrent RPC calls with internal locking
- Connection pool managed by coordinator

### Task Assignment Strategy
- Round-robin distribution with fallback to next available worker
- Hash-based partitioning ensures deterministic key distribution
- Automatic rebalancing on worker failure

## Authors
Shubham Sharma - MS Computer Science, UC Santa Cruz
