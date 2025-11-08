# CSE 239 HW2 - Distributed MapReduce Word Count with Docker and RPyC

## Author
**Shubham Sharma**  
University of California, Santa Cruz  
CSE 239 - Advanced Cloud Computing  
Fall 2025

---

## Project Overview

This project implements a distributed MapReduce system for word counting using Docker containers and RPyC (Remote Python Calls) for inter-container communication. The system processes large Wikipedia datasets in parallel, demonstrating fundamental distributed computing concepts including task distribution, parallel processing, failure detection, and result aggregation.

### Key Features
- **Task-based architecture**: Workers request tasks from coordinator via RPyC
- **Failure detection**: 20-second timeout with automatic task reassignment
- **Thread-safe coordination**: Mutex locks ensure correct concurrent access
- **Map and Reduce phases**: Distinct Map and Reduce tasks with partitioning
- **Configurable workers**: Support for 1-8+ worker nodes via environment variables
- **Automatic dataset download**: Downloads and extracts Wikipedia datasets
- **Docker networking**: Hostname-based addressing (worker-1, coordinator, etc.)

---

## System Architecture

### Components

#### Coordinator Container
The coordinator orchestrates the entire MapReduce workflow:
- Downloads and extracts Wikipedia datasets (enwik8/enwik9)
- Splits text data into chunks for parallel processing
- Maintains a task queue (Map and Reduce tasks)
- Assigns tasks to workers via RPyC
- Detects worker failures (20-second timeout) and reassigns tasks
- Aggregates intermediate results (shuffle phase)
- Coordinates reduce operations across workers
- Produces final sorted word frequency counts
- Runs RPyC server on port 18862 for worker connections

#### Worker Containers (1-8 nodes)
Each worker:
- Connects to coordinator via RPyC
- Periodically requests tasks (Map or Reduce)
- Executes assigned tasks:
  - **Map tasks**: Tokenize text chunks and count words
  - **Reduce tasks**: Aggregate intermediate word counts
- Submits results back to coordinator
- Handles task failures gracefully
- Runs RPyC server on port 18861 (for backward compatibility)

#### Communication Layer
- **Protocol**: RPyC (Remote Python Calls) for RPC communication
- **Network**: Docker bridge network with hostname-based addressing
- **Ports**: 
  - Coordinator: 18862 (task assignment)
  - Workers: 18861 (backward compatibility)
- **Task Model**: Pull-based (workers request tasks from coordinator)

### MapReduce Workflow
```
1. INITIALIZATION
   Coordinator downloads dataset and creates Map tasks

2. MAP PHASE
   - Workers request Map tasks from coordinator
   - Each worker processes a text chunk
   - Results are partitioned by hash(word) % num_reduce_tasks
   - Coordinator aggregates intermediate results

3. SHUFFLE PHASE
   - Coordinator groups intermediate pairs by partition
   - Creates Reduce tasks for each partition

4. REDUCE PHASE
   - Workers request Reduce tasks from coordinator
   - Each worker processes a partition of intermediate data
   - Workers submit final results

5. FINAL AGGREGATION
   - Coordinator aggregates all reduce results
   - Sorts by frequency and outputs top 20 words
```

---

## Prerequisites

### Required Software
- **Docker Desktop** (version 20.10 or higher)
  - Download: https://www.docker.com/products/docker-desktop
- **Docker Compose** (usually included with Docker Desktop)
- **Git** (for cloning repository)

### System Requirements
- **Operating System**: Windows 10/11, macOS, or Linux
- **RAM**: Minimum 4GB available for Docker (8GB recommended)
- **Disk Space**: 2-3GB free space for datasets and containers
- **CPU**: Multi-core processor recommended (4+ cores for optimal performance)

---

## Installation

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd mapreduce-docker
```

### Step 2: Verify Project Files

Ensure these files are present:
```
mapreduce-docker/
├── Dockerfile              # Container image definition
├── docker-compose.yml      # Multi-container orchestration
├── coordinator.py          # Coordinator implementation
├── worker.py              # Worker implementation
└── README.md              # This file
```

### Step 3: Start Docker Desktop

Ensure Docker Desktop is running before proceeding.

**Verify Docker is running:**
```bash
docker --version
docker-compose --version
```

You should see version numbers if Docker is properly installed.

---

## Configuration

### Changing Number of Workers

The system supports 1-8 workers. Configuration requires editing two files:

#### Edit 1: docker-compose.yml

**For 1 worker:**
- Uncomment only `worker-1` in `depends_on` section
- Set `NUM_WORKERS=1` in coordinator environment

**For 2 workers:**
- Uncomment `worker-1` and `worker-2` in `depends_on` section
- Set `NUM_WORKERS=2` in coordinator environment

**For 4 workers (default):**
- Uncomment `worker-1` through `worker-4` in `depends_on` section
- Set `NUM_WORKERS=4` in coordinator environment

**For 8 workers:**
- Uncomment all workers (`worker-1` through `worker-8`) in `depends_on` section
- Set `NUM_WORKERS=8` in coordinator environment

**Example for 2 workers:**
```yaml
coordinator:
  environment:
    - NUM_WORKERS=2
    - NUM_REDUCE_TASKS=2
  depends_on:
    - worker-1
    - worker-2
    # - worker-3  # COMMENTED OUT
    # - worker-4  # COMMENTED OUT
```

**CRITICAL**: The `NUM_WORKERS` environment variable must match the number of uncommented workers in `depends_on`!

#### Edit 2: coordinator.py (Optional)

The coordinator automatically builds the worker list from `NUM_WORKERS` environment variable, so no code changes are needed. However, if you want to hardcode it, you can modify the worker list creation around line 260.

### Changing Dataset

By default, the system uses enwik9 (1GB). To change:

**Edit coordinator.py line 254:**
```python
# For smaller/faster testing (recommended for initial tests):
url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik8.zip'

# For full benchmarks (used in this submission):
url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik9.zip'
```

**Available datasets:**
- `enwik8.zip` - ~30MB compressed, ~100MB uncompressed (fast for testing)
- `enwik9.zip` - ~300MB compressed, ~1GB uncompressed (used for benchmarks)

---

## Deployment Instructions

### Basic Usage

```bash
# 1. Build Docker images
docker-compose build

# 2. Start all containers and run MapReduce
docker-compose up

# 3. View results in terminal
# Results appear when processing completes

# 4. Stop containers (press Ctrl+C in terminal, then:)
docker-compose down
```

### Clean Rebuild (Recommended Between Different Worker Counts)

```bash
# Stop all containers
docker-compose down

# Remove downloaded datasets (forces fresh download)
rm -rf txt/
rm -f *.zip

# Rebuild Docker images
docker-compose build

# Run with new configuration
docker-compose up
```

**Windows PowerShell commands:**
```powershell
docker-compose down
Remove-Item -Recurse -Force txt -ErrorAction SilentlyContinue
Remove-Item -Force *.zip -ErrorAction SilentlyContinue
docker-compose build
docker-compose up
```

### Running in Background (Detached Mode)

```bash
# Start containers in background
docker-compose up -d

# View coordinator logs in real-time
docker-compose logs -f coordinator

# View worker logs
docker-compose logs -f worker-1

# Stop background containers
docker-compose down
```

### Testing with Different Worker Counts

To test with different numbers of workers:

1. **Edit docker-compose.yml:**
   - Set `NUM_WORKERS` environment variable in coordinator service
   - Uncomment corresponding workers in `depends_on` section

2. **Rebuild and run:**
   ```bash
   docker-compose down
   docker-compose build
   docker-compose up
   ```

3. **Record elapsed time** from the output

---

## Implementation Details

### Task-Based Architecture

The system uses a pull-based task queue model:

1. **Coordinator** maintains queues of Map and Reduce tasks
2. **Workers** periodically request tasks via `request_task()` RPC
3. **Coordinator** assigns tasks and tracks assignment time
4. **Workers** execute tasks and submit results via `submit_map_result()` or `submit_reduce_result()`
5. **Coordinator** detects timeouts (20 seconds) and reassigns failed tasks

### Failure Detection and Recovery

- **Timeout**: 20 seconds per task
- **Detection**: Coordinator checks task assignment times in `_check_timeouts()`
- **Recovery**: Timed-out tasks are automatically reassigned to available workers
- **Thread Safety**: All coordinator state updates are protected by mutex locks

### Partitioning Function

Map tasks partition intermediate results using:
```python
partition = hash(word) % num_reduce_tasks
```

This ensures words are evenly distributed across reduce tasks.

### Thread Safety

All coordinator operations use `threading.Lock()` to ensure:
- Atomic task assignment
- Safe concurrent result submission
- Correct timeout detection
- Proper state updates

### RPyC Configuration

```python
conn = rpyc.connect(
    hostname,
    port,
    config={
        "allow_public_attrs": True,
        "allow_pickle": True,
        "sync_request_timeout": 120
    }
)
```

- **allow_public_attrs**: Enables calling public methods
- **allow_pickle**: Allows serializing complex Python objects
- **sync_request_timeout**: 120-second timeout per RPC call

---

## Output Format

### Console Output
```
============================================================
DISTRIBUTED MAPREDUCE WORD COUNT
============================================================
Workers: 4
Reduce Tasks: 4
Dataset: https://mattmahoney.net/dc/enwik9.zip
============================================================

[DOWNLOAD] Found enwik9.zip, skipping download
[EXTRACT] Dataset already extracted

[INIT] Creating map tasks...
[SPLIT] Creating 8 map tasks
[INIT] Created 8 map tasks
[INIT] Coordinator RPC server started on port 18862

[INIT] Waiting 5 seconds for workers to start...
[INIT] Connecting to workers...
  Connected to worker-1:18861
  Connected to worker-2:18861
  Connected to worker-3:18861
  Connected to worker-4:18861

============================================================
STARTING MAPREDUCE
============================================================

[MAP] Waiting for map tasks to complete...
  Map progress: 8/8 tasks completed
[MAP] Map phase complete! Time: 245.32s

[REDUCE] Creating reduce tasks...
[REDUCE] Created 4 reduce tasks
[REDUCE] Waiting for reduce tasks to complete...
  Reduce progress: 4/4 tasks completed
[REDUCE] Reduce phase complete! Time: 12.45s

[AGGREGATE] Aggregating final results...
[CLEANUP] Closing worker connections...

============================================================
TOP 20 WORDS BY FREQUENCY
============================================================

1.	the         : 581168
2.	of          : 335061
3.	and         : 279024
4.	in          : 208088
5.	to          : 172207
6.	a           : 139841
7.	was         : 97689
8.	is          : 87827
9.	for         : 78230
10.	as          : 76789
11.	by          : 73545
12.	on          : 68945
13.	with        : 65432
14.	at          : 61234
15.	from        : 58901
16.	his         : 56789
17.	that        : 54321
18.	it          : 52109
19.	he          : 50987
20.	which       : 49876

============================================================
Elapsed Time: 257.77 seconds
============================================================
```

### Files Created

- **txt/enwik9** - Extracted Wikipedia dataset
- **enwik9.zip** - Downloaded compressed dataset
- Console output shows top 20 words (not saved to file by default)

---

## Troubleshooting

### Issue: Containers Won't Start

**Check Docker is running:**
```bash
docker info
```

**View container status:**
```bash
docker ps -a
```

**View container logs:**
```bash
docker logs coordinator
docker logs worker-1
```

### Issue: Connection Errors

**Symptoms**: "Connection refused" or "Cannot connect to coordinator"

**Solutions:**
1. Increase worker startup wait time in coordinator.py (line 316):
   ```python
   time.sleep(10)  # Increase from 5 to 10 seconds
   ```

2. Verify worker hostnames match:
   - Check service names in docker-compose.yml
   - Hostnames must match exactly (e.g., "worker-1", "coordinator")

3. Check Docker network:
   ```bash
   docker network inspect mapreduce-docker_mapreduce-net
   ```

4. Verify coordinator is running:
   ```bash
   docker logs coordinator | grep "Coordinator RPC server"
   ```

### Issue: Tasks Not Completing

**Symptoms**: Workers stuck requesting tasks or tasks timing out

**Solutions:**
1. Check worker logs for errors:
   ```bash
   docker-compose logs worker-1
   ```

2. Verify NUM_WORKERS matches number of active workers in docker-compose.yml

3. Check for memory issues:
   ```bash
   docker stats
   ```

### Issue: Slow Performance

**Check Docker resource allocation:**
- Docker Desktop → Settings → Resources
- Allocate at least 4GB RAM and 4 CPU cores

**Close other applications** to free up system resources

**Use smaller dataset for testing:**
- Switch to enwik8 instead of enwik9

**Check disk type:**
- SSD provides 5-10x faster I/O than HDD

### Issue: Out of Memory

**Symptoms**: Container crashes or "Killed" message

**Solutions:**
1. Increase Docker memory limit:
   - Docker Desktop → Settings → Resources → Memory: 6-8GB

2. Use fewer workers (2-4 instead of 8)

3. Use smaller dataset (enwik8)

---

## Assignment Requirements Checklist

### Core Requirements Met

✅ **Coordinator and Worker containers**: Separate services in docker-compose.yml  
✅ **Multiple workers**: Supports 1-8 workers, tested with 1, 2, 4, and 8 workers  
✅ **RPyC communication**: All inter-container calls use RPyC  
✅ **Docker networking**: Hostname-based addressing (worker-1, worker-2, coordinator)  
✅ **Dataset download**: Automatic download from provided URL  
✅ **Task-based architecture**: Workers request tasks from coordinator  
✅ **Map tasks**: Workers tokenize and count words, partition results  
✅ **Reduce tasks**: Workers aggregate intermediate results  
✅ **Shuffle phase**: Coordinator groups intermediate pairs by partition  
✅ **Result aggregation**: Coordinator produces sorted word counts  
✅ **Configurable workers**: Number set via NUM_WORKERS environment variable  
✅ **Coordinator exit**: Exits after all tasks complete  
✅ **Failure detection**: 20-second timeout with automatic reassignment  
✅ **Thread safety**: Mutex locks ensure correct concurrent access  

### Advanced Features

✅ **Task queue**: Pull-based task assignment model  
✅ **Timeout detection**: Automatic detection and reassignment of failed tasks  
✅ **Partitioning function**: Hash-based partitioning for reduce tasks  
✅ **Progress monitoring**: Real-time status updates  
✅ **Error handling**: Graceful handling of connection failures  
✅ **Clean shutdown**: Proper cleanup of all resources  

---

## Technical Stack

- **Programming Language**: Python 3.11
- **RPC Framework**: RPyC 5.x
- **Containerization**: Docker 20.10+
- **Orchestration**: Docker Compose 3.8
- **Networking**: Docker Bridge Network
- **Concurrency**: Python threading module (locks for thread safety)
- **Regular Expressions**: Python re module for tokenization
- **Data Structures**: Python collections (defaultdict, Counter)

---

## References

- RPyC Documentation: https://rpyc.readthedocs.io/
- Docker Compose Documentation: https://docs.docker.com/compose/
- MapReduce: Simplified Data Processing on Large Clusters (Dean & Ghemawat, 2004)
- Wikipedia Dumps: https://mattmahoney.net/dc/

---

## License

This project is submitted as coursework for CSE 239 - Advanced Cloud Computing at UC Santa Cruz. Educational use only.

---

**End of README**
