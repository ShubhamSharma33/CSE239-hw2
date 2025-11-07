# CSE 239 - Distributed MapReduce Word Count with Docker

## Project Overview

A distributed MapReduce system implementing word count analysis across multiple Docker containers using RPyC (Remote Python Calls) for inter-container communication. The system processes large text datasets (Wikipedia dumps) in parallel, demonstrating key distributed computing concepts including task distribution, fault tolerance, and parallel processing.

## Author

Shubham Sharma  
UC Santa Cruz - CSE 239: Advanced Cloud Computing  
Fall 2025

## Architecture

### Components

**Coordinator Container**
- Orchestrates the entire MapReduce workflow
- Downloads and extracts datasets
- Streams files in chunks to avoid memory overflow
- Distributes map tasks across workers using round-robin scheduling
- Maintains persistent connections to workers for efficiency
- Aggregates results from all workers
- Implements asynchronous RPyC calls for non-blocking parallel execution

**Worker Containers**
- Run RPyC servers listening on port 18861
- Execute map operations: tokenize text and count word frequencies
- Return results as dictionaries for efficient aggregation
- Multiple workers (1-8) can run simultaneously

**Communication Layer**
- RPyC (Remote Python Calls) for RPC communication
- Docker's bridge network allows hostname-based addressing
- Asynchronous calls enable true parallelism without threading overhead

### Key Design Decisions

1. **Streaming File Processing**: Files are read in 50MB chunks rather than loading entirely into memory, enabling processing of multi-GB datasets
2. **Persistent Connections**: Worker connections are established once and reused throughout execution
3. **Asynchronous RPyC**: Non-blocking async calls allow all workers to process simultaneously
4. **Direct Aggregation**: Map phase returns word counts that are directly aggregated, eliminating the need for a separate reduce phase
5. **Round-Robin Distribution**: Tasks are distributed evenly across workers for load balancing

## Prerequisites

- Docker Desktop installed and running
- At least 4GB RAM available for Docker
- 2GB free disk space for datasets
- Python 3.9+ (included in Docker image)
- Git (for cloning repository)

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/ShubhamSharma33/CSE239-hw2.git
cd CSE239-hw2
```

### 2. Verify Required Files

Ensure the following files are present:
- `Dockerfile` - Defines the container image
- `docker-compose.yml` - Orchestrates multiple containers
- `coordinator.py` - Coordinator implementation
- `worker.py` - Worker implementation
- `README.md` - This file

## Configuration

### Changing Number of Workers

The system supports 1-8 workers. To configure:

#### Step 1: Edit `coordinator.py`

Modify the `WORKERS` list:
```python
# For 1 worker:
WORKERS = [
    ("worker-1", 18861),
]

# For 2 workers:
WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
]

# For 4 workers:
WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
    ("worker-3", 18861),
    ("worker-4", 18861),
]

# For 8 workers:
WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
    ("worker-3", 18861),
    ("worker-4", 18861),
    ("worker-5", 18861),
    ("worker-6", 18861),
    ("worker-7", 18861),
    ("worker-8", 18861),
]
```

#### Step 2: Edit `docker-compose.yml`

Uncomment/comment worker services to match the number in `WORKERS` list.

For **1 worker**, keep only:
```yaml
services:
  worker-1:
    # ... config ...
  
  # worker-2:  # Commented out
  #   # ... config ...
  
  coordinator:
    depends_on:
      - worker-1
```

For **4 workers**, keep workers 1-4 uncommented:
```yaml
services:
  worker-1:
    # ... config ...
  worker-2:
    # ... config ...
  worker-3:
    # ... config ...
  worker-4:
    # ... config ...
  
  coordinator:
    depends_on:
      - worker-1
      - worker-2
      - worker-3
      - worker-4
```

**CRITICAL**: The number of workers in `WORKERS` list must match the active workers in `docker-compose.yml`!

### Changing Dataset

By default, the system downloads `enwik8` (~30MB). To use a different dataset:

Edit `coordinator.py` line 17:
```python
def download(url='https://mattmahoney.net/dc/enwik8.zip'):
```

Available datasets:
- `enwik8.zip` - 100MB uncompressed (recommended for testing)
- `enwik9.zip` - 1GB uncompressed (for final benchmarks)

## Deployment & Execution

### Basic Workflow
```bash
# 1. Build Docker images
docker-compose build

# 2. Start all containers
docker-compose up

# 3. View output in terminal (results appear when complete)

# 4. Stop containers (press Ctrl+C, then:)
docker-compose down
```

### Clean Rebuild (Recommended Between Tests)
```bash
# Stop containers
docker-compose down

# Remove cached data
rm -rf txt/
rm -f *.zip

# Rebuild from scratch
docker-compose build --no-cache

# Run
docker-compose up
```

### Running in Background (Detached Mode)
```bash
# Start in background
docker-compose up -d

# View logs
docker-compose logs -f coordinator

# Stop when done
docker-compose down
```

## Benchmarking for Assignment

To complete Part (a) of the assignment, run with 1, 2, 4, and 8 workers:

### Test 1: 1 Worker
```bash
# Edit coordinator.py: Set WORKERS to 1 worker
# Edit docker-compose.yml: Keep only worker-1

docker-compose down
rm -rf txt/ && rm -f *.zip
docker-compose build
docker-compose up

# RECORD THE ELAPSED TIME FROM OUTPUT
```

### Test 2: 2 Workers
```bash
# Edit coordinator.py: Set WORKERS to 2 workers
# Edit docker-compose.yml: Enable worker-1 and worker-2

docker-compose down
rm -rf txt/ && rm -f *.zip
docker-compose build
docker-compose up

# RECORD THE ELAPSED TIME
```

### Test 3: 4 Workers
```bash
# Edit coordinator.py: Set WORKERS to 4 workers
# Edit docker-compose.yml: Enable workers 1-4

docker-compose down
rm -rf txt/ && rm -f *.zip
docker-compose build
docker-compose up

# RECORD THE ELAPSED TIME
```

### Test 4: 8 Workers
```bash
# Edit coordinator.py: Set WORKERS to 8 workers
# Edit docker-compose.yml: Enable all 8 workers

docker-compose down
rm -rf txt/ && rm -f *.zip
docker-compose build
docker-compose up

# RECORD THE ELAPSED TIME
```

## Output

### Console Output

The system provides detailed progress information:
```
============================================================
MEMORY-OPTIMIZED MAPREDUCE WORD COUNT
Using: Async RPyC + Streaming Files + Persistent Connections
============================================================

Connecting to 4 workers...
  ✓ Connected to worker-1:18861
  ✓ Connected to worker-2:18861
  ✓ Connected to worker-3:18861
  ✓ Connected to worker-4:18861

============================================================
STARTING MAP PHASE WITH ASYNC CALLS
============================================================

Processing file: txt/enwik8
  Dispatched 10 chunks, 8 pending...
  Dispatched 20 chunks, 12 pending...

✓ Map phase complete!
  Total chunks processed: 28
  Unique words found: 243426
  Time taken: 15.34 seconds

============================================================
TOP 20 MOST FREQUENT WORDS
============================================================
 1. the              581168
 2. of               335061
 3. and              279024
 4. in               208088
 5. to               172207
...

============================================================
✓ Complete! Saved results to txt/results.txt
✓ Total unique words: 243,426
✓ TOTAL ELAPSED TIME: 18.92 seconds
============================================================
```

### File Output

Results are saved to `txt/results.txt` with format:
```
word1    count1
word2    count2
...
```

## Features Implemented

### Core Requirements

✅ **Separate Docker Containers**: Coordinator and workers run in isolated containers  
✅ **Multiple Workers**: Supports 3+ workers (tested up to 8)  
✅ **RPyC Communication**: All inter-container communication uses RPyC  
✅ **Docker Networking**: Containers communicate via hostnames (worker-1, worker-2, etc.)  
✅ **Dataset Download**: Coordinator automatically downloads and extracts datasets  
✅ **Task Distribution**: Workers receive tasks dynamically via RPyC calls  
✅ **Result Aggregation**: Coordinator aggregates all worker outputs  
✅ **Configurable Workers**: Number of workers set via code configuration  
✅ **Completion Detection**: Coordinator exits when all tasks complete  

### Advanced Optimizations

✅ **Streaming File Processing**: Handles multi-GB files without excessive memory usage  
✅ **Asynchronous Execution**: Non-blocking RPyC calls enable true parallelism  
✅ **Persistent Connections**: Eliminates connection overhead between tasks  
✅ **Backpressure Control**: Prevents overwhelming workers with too many tasks  
✅ **Round-Robin Load Balancing**: Even distribution of work across workers  
✅ **Progress Monitoring**: Real-time status updates during execution  

### Fault Tolerance

✅ **Timeout Handling**: RPyC configured with 90-second timeout per operation  
✅ **Connection Error Detection**: Failed worker connections are reported  
✅ **Graceful Shutdown**: Proper cleanup of all connections on exit  

## Performance Characteristics

### Expected Performance (enwik8 dataset, ~100MB)

| Workers | Estimated Time | Speedup |
|---------|----------------|---------|
| 1       | 60-80 seconds  | 1.0x    |
| 2       | 35-50 seconds  | 1.5-2x  |
| 4       | 20-30 seconds  | 2.5-3x  |
| 8       | 15-25 seconds  | 3-4x    |

**Note**: Speedup is less than linear due to:
- Coordination overhead
- Network communication costs
- Amdahl's Law (some operations cannot be parallelized)

### Memory Usage

- **Streaming mode**: ~50-100MB regardless of dataset size
- **Worker containers**: ~50MB each
- **Total system**: ~300-500MB for 4 workers

## Troubleshooting

### Containers Won't Start
```bash
# Check Docker Desktop is running
docker info

# View container status
docker ps -a

# View container logs
docker logs coordinator
docker logs worker-1
```

### Connection Errors
```bash
# Ensure workers are ready (wait longer)
# Verify hostnames in coordinator.py match docker-compose.yml
# Check Docker network:
docker network inspect cse239-hw2_mapreduce-net
```

### Memory Issues
```bash
# Allocate more memory to Docker Desktop
# Settings → Resources → Memory: 4-6GB

# Use smaller dataset (enwik8 instead of enwik9)
# Reduce STREAM_CHUNK_SIZE in coordinator.py
```

### Slow Performance
```bash
# Close other applications
# Increase Docker CPU allocation
# Use SSD instead of HDD for better I/O
# Ensure no other Docker containers running
```

## Technical Details

### RPyC Configuration
```python
rpyc.connect(
    hostname, 
    port,
    config={
        "allow_public_attrs": True,
        "sync_request_timeout": 90
    }
)
```

### Chunking Strategy

Files are split into 50MB chunks for optimal performance:
- Small enough to distribute evenly
- Large enough to minimize overhead
- Prevents memory overflow on large files

### Word Tokenization
```python
words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
```

Extracts lowercase alphabetic words only, ignoring:
- Numbers
- Punctuation
- Special characters

## Project Structure
```
CSE239-hw2/
├── Dockerfile              # Container image definition
├── docker-compose.yml      # Multi-container orchestration
├── coordinator.py          # Main MapReduce coordinator
├── worker.py              # Worker node implementation
├── README.md              # This file
└── txt/                   # Created at runtime for datasets
    ├── enwik8             # Downloaded dataset
    └── results.txt        # Output word counts
```

## System Requirements Met

| Requirement | Implementation |
|------------|----------------|
| Coordinator + Workers in Docker | ✅ Separate containers with docker-compose |
| 3+ Workers | ✅ Supports 1-8 workers |
| RPyC Communication | ✅ All inter-container calls use RPyC |
| Docker Networking | ✅ Bridge network with hostnames |
| Dataset Download | ✅ Automatic download and extraction |
| Task Distribution | ✅ Async RPyC calls to workers |
| Result Aggregation | ✅ Coordinator collects and sorts results |
| Configurable Workers | ✅ Edit WORKERS list in coordinator.py |
| Timeout Handling | ✅ 90-second timeout per operation |
| Coordinator Exit | ✅ Exits after all tasks complete |

## References

- RPyC Documentation: https://rpyc.readthedocs.io/
- Docker Compose: https://docs.docker.com/compose/
- MapReduce Paper: Dean & Ghemawat (2004)

## License

MIT License - Educational use for CSE 239

## Support

For issues or questions:
- Check troubleshooting section above
- Review Docker logs: `docker-compose logs`
- Verify file configurations match examples

---