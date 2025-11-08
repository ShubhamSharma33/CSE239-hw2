# Dockerized MapReduce Word Count

## Author

Shubham Sharma  
CSE 239 - Advanced Cloud Computing  
UC Santa Cruz

A distributed MapReduce implementation using Docker containers and RPyC for large-scale word frequency analysis.

## Architecture Overview

The system implements a distributed MapReduce framework with:
- **1 Coordinator Container**: Orchestrates task distribution, handles worker failures, and aggregates final results
- **1-8 Worker Containers**: Execute Map and Reduce tasks in parallel
- **Docker Networking**: Enables hostname-based communication between containers
- **RPyC (Remote Python Calls)**: Provides RPC mechanism for coordinator-worker interaction

## Prerequisites

- Docker Engine (20.10+)
- Docker Compose (v3.8+)
- Python 3.10
- Network connection for dataset download
- ~2GB free disk space (for enwik9 dataset)

## Project Structure
```
.
├── coordinator.py      # Coordinator logic with MapReduce orchestration
├── worker.py          # Worker service for Map and Reduce operations
├── docker-compose.yml # Docker orchestration for 8 workers
├── Dockerfile        # Container image definition
└── README.md        # This file
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/ShubhamSharma33/CSE239-hw2.git
cd CSE239-hw2
```

2. Build Docker images:
```bash
docker-compose build
```

## Running MapReduce Jobs

### Default Configuration (8 Workers)
```bash
docker-compose up --build
```

### Variable Worker Configurations

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

### Using Different Datasets

Default dataset is enwik9 (~1GB). To use enwik8 (~100MB) for testing:
```bash
docker-compose run -e DATA_URL="https://mattmahoney.net/dc/enwik8.zip" coordinator
```

## Implementation Details

### Core Components

#### Coordinator (`coordinator.py`)
- **Variable Management**: 
  - `worker_connections`: List of active RPyC connections to workers
  - `num_chunks`: Number of text segments (default: workers × 8)
  - `thread_pool_size`: Concurrent execution threads
  - `grouped`: Shuffled intermediate key-value pairs
- **Phases**:
  1. **Initialization**: Downloads dataset, establishes worker connections
  2. **Map Phase**: Distributes text chunks across workers using ThreadPoolExecutor
  3. **Shuffle Phase**: Groups intermediate results by key
  4. **Reduce Phase**: Partitions grouped data using hash function, sends to workers
  5. **Aggregation**: Combines reduced results into final word counts

#### Worker (`worker.py`)
- **Map Function**: 
  - Tokenizes text chunks
  - Removes punctuation using `PUNCT_TRANSLATOR`
  - Filters stop words from `STOP_WORDS` set
  - Returns dictionary of word frequencies
- **Reduce Function**:
  - Receives `word_groups` (lists of counts per word)
  - Sums all counts for each word
  - Returns consolidated dictionary

### Key Features

1. **Fault Tolerance**
   - Automatic retry logic with round-robin worker reassignment
   - Maximum retries equal to number of available workers
   - 20-second timeout detection (configurable to 250s for large datasets)
   - Failed tasks reassigned to next available worker

2. **Load Balancing**
   - Text split into `num_chunks` (8× workers) for fine-grained distribution
   - Hash-based partitioning ensures even reduce workload
   - Parallel execution via ThreadPoolExecutor

3. **Communication Protocol**
   - RPyC configuration with pickle serialization enabled
   - Sync request timeout: 250 seconds
   - All workers listen on port 18865
   - Hostname resolution through Docker network

4. **Mutual Exclusion**
   - Collections.Counter ensures thread-safe aggregation
   - ThreadPoolExecutor manages concurrent worker calls
   - No shared state between workers (stateless design)

### Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| Worker Count | 8 | Number of worker containers (1-8) |
| Chunks | workers×8 | Text segments for distribution |
| Thread Pool | min(chunks, workers×8) | Concurrent execution threads |
| Timeout | 250s | RPyC sync request timeout |
| Worker Port | 18865 | RPyC service port |

## Performance Results

Testing with enwik8 dataset on M4 Mac:

| Workers | Execution Time | Speedup | Efficiency |
|---------|---------------|---------|------------|
| 1 | 22.2 seconds | 1.00× | 100% |
| 2 | 13.5 seconds | 1.65× | 82.5% |
| 4 | 9.6 seconds | 2.31× | 57.8% |
| 8 | 7.5 seconds | 2.95× | 36.9% |

## Monitoring & Debugging

View real-time logs:
```bash
# All containers
docker-compose logs -f

# Specific worker
docker logs worker-1 -f

# Coordinator only
docker logs coordinator -f
```

Check container status:
```bash
docker-compose ps
```

## Cleanup

Remove containers and network:
```bash
docker-compose down
```

Clean everything including images:
```bash
docker-compose down --rmi all --volumes
rm -rf txt/
```

## Requirements Compliance Checklist

**Coordinator and Worker in separate Docker containers** - Implemented via docker-compose.yml  
**Multiple workers (3+)** - Supports 1-8 workers via configuration  
**RPyC communication with hostname resolution** - Workers accessible via "worker-1" through "worker-8"  
**URL-based dataset download** - Downloads UTF-8 Wikipedia dumps via HTTP  
**Map tasks with partitioning** - Hash-based partitioning into regions  
**Reduce tasks for aggregation** - Parallel reduction of intermediate values  
**Mutual exclusion for concurrent updates** - ThreadPoolExecutor ensures safe concurrent access  
**Failed task reassignment** - Retry logic with 20s timeout (configurable)  
**Configurable worker count** - Via commenting in coordinator.py and docker-compose.yml  
**Coordinator exits after completion** - Terminates when all tasks finish  

## Troubleshooting

**Workers not connecting:**
- Ensure all worker containers are running: `docker-compose ps`
- Check Docker network: `docker network ls`

**Out of memory:**
- Use smaller dataset (enwik8 instead of enwik9)
- Reduce number of chunks

**Slow performance:**
- Increase timeout in RPyC config if processing large datasets
- Ensure Docker has sufficient resources allocated

