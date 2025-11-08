# Dockerized MapReduce Word Count

A distributed MapReduce implementation using Docker containers and RPyC for word frequency analysis.

## Architecture

The system consists of:
- **1 Coordinator**: Manages task distribution, handles failures, and aggregates results
- **N Workers** (1-8): Execute Map and Reduce tasks in parallel
- **Docker Network**: Enables container communication via hostnames
- **RPyC**: Remote Python Calls for coordinator-worker communication

## Requirements

- Docker & Docker Compose
- Python 3.10
- Internet connection (for dataset download)

## Installation & Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd mapreduce-docker
```

2. Ensure all files are present:
```
├── Dockerfile
├── docker-compose.yml
├── coordinator.py
├── worker.py
└── README.md
```

## Running the MapReduce System

### Quick Start (8 workers)

```bash
# Build and run with default 8 workers
docker-compose up --build

# Clean up after execution
docker-compose down
```

### Testing Different Worker Configurations

#### 1 Worker:
```bash
docker-compose -f docker-compose-1worker.yml up --build
```

#### 2 Workers:
```bash
docker-compose -f docker-compose-2worker.yml up --build
```

#### 4 Workers:
```bash
docker-compose -f docker-compose-4worker.yml up --build
```

#### 8 Workers (default):
```bash
docker-compose up --build
```

### Using Different Datasets

```bash
# Default: enwik9 (~1GB)
docker-compose up --build

# For testing with smaller dataset (enwik8 ~100MB):
docker-compose run -e DATA_URL="https://mattmahoney.net/dc/enwik8.zip" coordinator
```

## Implementation Details

### Key Features

1. **Parallel Processing**
   - Map phase: Chunks distributed across workers using ThreadPoolExecutor
   - Reduce phase: Partitioned data processed in parallel
   - Configurable chunk size: n_workers * 8 for optimal load balancing

2. **Fault Tolerance**
   - Retry logic with maximum attempts equal to number of workers
   - Failed tasks automatically reassigned to available workers
   - 20-second timeout detection for unresponsive workers

3. **Communication Protocol**
   - RPyC with pickle serialization enabled
   - Sync request timeout: 300 seconds
   - Port 18865 for all workers

4. **Data Processing**
   - Stop words filtering (common English words)
   - Punctuation removal and case normalization
   - Line-based text splitting to preserve word boundaries

### Coordinator Workflow

1. Downloads and extracts dataset (if not present)
2. Reads text file into memory
3. Splits text into n_workers * 8 chunks
4. **MAP Phase**: Distributes chunks to workers in parallel
5. **SHUFFLE Phase**: Groups intermediate results by key
6. **REDUCE Phase**: Partitions grouped data and sends to workers
7. **AGGREGATION**: Combines all reduced results
8. Outputs top 20 words by frequency

### Worker Operations

- **Map Function**: 
  - Tokenizes text chunk
  - Removes punctuation and stop words
  - Counts word frequencies
  - Returns dictionary of word counts

- **Reduce Function**:
  - Receives grouped word-value pairs
  - Sums all values for each word
  - Returns final counts dictionary

## Performance Results

| Workers | Execution Time | Speedup |
|---------|---------------|---------|
| 1       | ~X seconds    | 1.0x    |
| 2       | ~X seconds    | ~X.Xx   |
| 4       | ~X seconds    | ~X.Xx   |
| 8       | ~X seconds    | ~X.Xx   |

## Monitoring & Debugging

View logs from all containers:
```bash
docker-compose logs -f
```

View specific worker logs:
```bash
docker logs worker-1 -f
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

Remove downloaded datasets:
```bash
rm -rf txt/
```

Remove all containers and images:
```bash
docker-compose down --rmi all --volumes
```

## Requirements Checklist

✅ Coordinator and Workers in separate Docker containers  
✅ Multiple workers (3+) configurable via docker-compose  
✅ RPyC communication with Docker hostname resolution  
✅ URL-based dataset download (UTF-8)  
✅ Map tasks with partitioning into regions  
✅ Reduce tasks for aggregating intermediate values  
✅ Mutual exclusion for concurrent updates  
✅ Failed task reassignment with timeout detection  
✅ Configurable number of workers (1-8)  

## Authors

[Your Name]  
CSE 239 - Advanced Cloud Computing  
UC Santa Cruz