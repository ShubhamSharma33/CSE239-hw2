# Dockerized MapReduce Word Count System

## Overview
This assignment implements a distributed MapReduce system using Docker containers and RPyC (Remote Python Calls) for word counting on large text datasets. 

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
git clone https://github.com/ShubhamSharma33/CSE239-hw2.git
cd CSE239-hw2
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

###4. Variable Worker Configurations

To run with different numbers of workers, comment out unused workers in both `coordinator.py` and `docker-compose.yml`:

#### For 1 Worker:
In `coordinator.py`, comment out workers 2-8:
```python
WORKERS = [
    ("worker-1", 18865),
    # ("worker-2", 18865),
    # ("worker-3", 18865),
    # ... (comment out rest)
]
```

In `docker-compose.yml`, comment out worker services 2-8/remove comments from:
```yaml
services:
  worker-1:
    # ... configuration
  # worker-2:
  #   # ... commented out
  # worker-3:
  #   # ... commented out
```

and in the same file comment out/remove comments from:
```yaml
depends_on:
      - worker-1
      # - worker-2
      # - worker-3
      # - worker-4
      ...
      ...
```

Then run: `docker-compose up --build`

#### For 2 Workers:
Comment out workers 3-8 in both files and run: `docker-compose down` and then `docker-compose up`

#### For 4 Workers:
Comment out workers 5-8 in both files and run: `docker-compose down` and then `docker-compose up`

#### For 8 Workers:
Remove all commented workers in both files and run: `docker-compose down` and then `docker-compose up`


### 5. Stop the System
```bash
docker-compose down
```

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

### Using Different Datasets

Default dataset is enwik9 (~1GB). To use enwik8 (~100MB) for testing:
```bash
docker-compose run -e DATA_URL="https://mattmahoney.net/dc/enwik8.zip" coordinator
```

## Performance Results

Testing with enwik8 dataset on M4 Mac:

| Workers | Execution Time | Speedup |
|---------|---------------|---------|
| 1 | 100.04 seconds | 1.00× |
| 2 | 44.23 seconds | 2.26× |
| 4 | 25.02 seconds | 4.00× |
| 8 | 18.57 seconds | 5.39× |
 

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
