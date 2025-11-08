import rpyc
import string
import collections
import time
import operator
import glob
import sys
import urllib.request
import zipfile
import os
import threading
from enum import Enum
from typing import Optional, Dict, List, Tuple

class TaskType(Enum):
    MAP = "map"
    REDUCE = "reduce"
    DONE = "done"

class Task:
    def __init__(self, task_type: TaskType, task_id: int, data=None):
        self.task_type = task_type
        self.task_id = task_id
        self.data = data
        self.assigned_to = None
        self.assigned_at = None
        self.completed = False

# Global coordinator instance (will be set in main)
_coordinator_instance = None

class CoordinatorService(rpyc.Service):
    def exposed_request_task(self, worker_id: str) -> Optional[Dict]:
        """Worker requests a task. Returns task or None if no tasks available."""
        try:
            return _coordinator_instance._request_task(worker_id)
        except Exception as e:
            print(f"[COORDINATOR] Error in request_task from {worker_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def exposed_submit_map_result(self, task_id: int, result: Dict, worker_id: str) -> bool:
        """Worker submits map task result."""
        try:
            return _coordinator_instance._submit_map_result(task_id, result, worker_id)
        except Exception as e:
            print(f"[COORDINATOR] Error in submit_map_result from {worker_id}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def exposed_submit_reduce_result(self, task_id: int, result: Dict, worker_id: str) -> bool:
        """Worker submits reduce task result."""
        try:
            return _coordinator_instance._submit_reduce_result(task_id, result, worker_id)
        except Exception as e:
            print(f"[COORDINATOR] Error in submit_reduce_result from {worker_id}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def exposed_get_intermediate_data(self, partition: int) -> Dict:
        """Get intermediate data for a reduce partition."""
        return _coordinator_instance.get_intermediate_data(partition)

class Coordinator:
    def __init__(self):
        self.lock = threading.Lock()
        self.map_tasks = []
        self.reduce_tasks = []
        self.completed_map_tasks = {}
        self.completed_reduce_tasks = {}
        self.map_results = {}  # Store intermediate results by partition
        self.reduce_results = {}
        self.next_task_id = 0
        self.num_reduce_tasks = 0
        self.connected_workers = set()  # Track which workers have connected
        
    def _request_task(self, worker_id: str) -> Optional[Dict]:
        """Worker requests a task. Returns task or None if no tasks available."""
        with self.lock:
            # Track connected workers
            if worker_id not in self.connected_workers:
                self.connected_workers.add(worker_id)
                print(f"[COORDINATOR] Worker {worker_id} connected (total: {len(self.connected_workers)})")
            
            # Check for timed out tasks first
            self._check_timeouts()
            
            # Assign map task if available
            for task in self.map_tasks:
                if not task.completed and task.assigned_to is None:
                    task.assigned_to = worker_id
                    task.assigned_at = time.time()
                    return {
                        "task_type": task.task_type.value,
                        "task_id": task.task_id,
                        "data": task.data
                    }
            
            # If all map tasks done, assign reduce tasks
            if all(t.completed for t in self.map_tasks):
                for task in self.reduce_tasks:
                    if not task.completed and task.assigned_to is None:
                        task.assigned_to = worker_id
                        task.assigned_at = time.time()
                        return {
                            "task_type": task.task_type.value,
                            "task_id": task.task_id,
                            "data": task.data
                        }
            
            # All tasks done
            if all(t.completed for t in self.map_tasks) and all(t.completed for t in self.reduce_tasks):
                return {"task_type": "done"}
            
            return None
    
    def _submit_map_result(self, task_id: int, result: Dict, worker_id: str) -> bool:
        """Worker submits map task result."""
        with self.lock:
            # Find the task
            task = None
            for t in self.map_tasks:
                if t.task_id == task_id:
                    task = t
                    break
            
            if task is None:
                return False
            
            # Only accept if assigned to this worker and not already completed
            if task.assigned_to == worker_id and not task.completed:
                task.completed = True
                task.assigned_to = None
                task.assigned_at = None
                
                # Store intermediate results partitioned by reduce task
                for word, count in result.items():
                    partition = hash(word) % self.num_reduce_tasks
                    if partition not in self.map_results:
                        self.map_results[partition] = {}
                    if word not in self.map_results[partition]:
                        self.map_results[partition][word] = 0
                    self.map_results[partition][word] += count
                
                return True
            
            return False
    
    def _submit_reduce_result(self, task_id: int, result: Dict, worker_id: str) -> bool:
        """Worker submits reduce task result."""
        with self.lock:
            # Find the task
            task = None
            for t in self.reduce_tasks:
                if t.task_id == task_id:
                    task = t
                    break
            
            if task is None:
                return False
            
            # Only accept if assigned to this worker and not already completed
            if task.assigned_to == worker_id and not task.completed:
                task.completed = True
                task.assigned_to = None
                task.assigned_at = None
                self.completed_reduce_tasks[task_id] = result
                return True
            
            return False
    
    def _check_timeouts(self):
        """Check for tasks that have exceeded 20 second timeout and reassign them."""
        TIMEOUT = 20.0
        current_time = time.time()
        
        # Check map tasks
        for task in self.map_tasks:
            if not task.completed and task.assigned_to is not None:
                if current_time - task.assigned_at > TIMEOUT:
                    print(f"[TIMEOUT] Map task {task.task_id} timed out, reassigning...")
                    task.assigned_to = None
                    task.assigned_at = None
        
        # Check reduce tasks
        for task in self.reduce_tasks:
            if not task.completed and task.assigned_to is not None:
                if current_time - task.assigned_at > TIMEOUT:
                    print(f"[TIMEOUT] Reduce task {task.task_id} timed out, reassigning...")
                    task.assigned_to = None
                    task.assigned_at = None
    
    def is_done(self) -> bool:
        """Check if all tasks are completed."""
        with self.lock:
            return (all(t.completed for t in self.map_tasks) and 
                   all(t.completed for t in self.reduce_tasks))
    
    def get_final_results(self) -> Dict:
        """Aggregate all reduce results into final word counts."""
        with self.lock:
            final_counts = {}
            for result in self.completed_reduce_tasks.values():
                for word, count in result.items():
                    if word not in final_counts:
                        final_counts[word] = 0
                    final_counts[word] += count
            return final_counts
    
    def get_intermediate_data(self, partition: int) -> Dict:
        """Get intermediate data for a reduce partition."""
        with self.lock:
            return self.map_results.get(partition, {}).copy()  # Return copy for thread safety

def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    """Downloads and unzips a wikipedia dataset in txt/."""
    filename = url.split('/')[-1]
    zip_path = filename
    
    # Download if not exists
    if not os.path.exists(zip_path):
        print(f"[DOWNLOAD] Downloading {url}...")
        print(f"[DOWNLOAD] This may take several minutes for enwik9 (~300MB)...")
        urllib.request.urlretrieve(url, zip_path)
        print("[DOWNLOAD] Complete")
    else:
        print(f"[DOWNLOAD] Found {zip_path}, skipping download")
    
    # Extract if txt/ is empty
    os.makedirs('txt', exist_ok=True)
    if not glob.glob('txt/*'):
        print(f"[EXTRACT] Unzipping {zip_path}...")
        print(f"[EXTRACT] This will create a ~1GB file...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall('txt/')
        print("[EXTRACT] Complete")
    else:
        print("[EXTRACT] Dataset already extracted")
    
    return glob.glob('txt/*')

def split_files_into_chunks(files, n_map_tasks):
    """Split files into chunks for map tasks."""
    target_chunks = n_map_tasks
    
    print(f"[SPLIT] Creating {target_chunks} map tasks")
    
    # Calculate total file size
    total_size = 0
    for filepath in files:
        total_size += os.path.getsize(filepath)
    
    chunk_size = total_size // target_chunks
    print(f"[SPLIT] Total size: {total_size:,} bytes")
    print(f"[SPLIT] Target chunk size: {chunk_size:,} bytes (~{chunk_size/1024/1024:.1f} MB)")
    
    # Initialize chunk storage
    chunks = [[] for _ in range(target_chunks)]
    current_chunk_idx = 0
    current_chunk_size = 0
    
    # Read in blocks and distribute
    BLOCK_SIZE = 1024 * 1024  # 1MB blocks
    
    for filepath in files:
        print(f"  Streaming {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            while True:
                block = f.read(BLOCK_SIZE)
                if not block:
                    break
                
                chunks[current_chunk_idx].append(block)
                current_chunk_size += len(block)
                
                if current_chunk_size >= chunk_size and current_chunk_idx < target_chunks - 1:
                    current_chunk_idx += 1
                    current_chunk_size = 0
    
    # Join the blocks for each chunk
    print(f"[SPLIT] Assembling {target_chunks} chunks...")
    result = []
    for i, chunk_blocks in enumerate(chunks):
        assembled = ''.join(chunk_blocks)
        result.append(assembled)
        if (i + 1) % 4 == 0 or i == len(chunks) - 1:
            print(f"  Assembled {i+1}/{target_chunks} chunks (~{len(assembled)/1024/1024:.1f} MB)")
    
    print(f"[SPLIT] ✓ Ready: {len(result)} chunks, avg ~{(total_size/target_chunks)/1024/1024:.1f} MB each")
    return result

if __name__ == "__main__":
    # Get number of workers from environment or default to 4
    num_workers = int(os.environ.get('NUM_WORKERS', '4'))
    num_reduce_tasks = int(os.environ.get('NUM_REDUCE_TASKS', str(num_workers)))
    
    # Build worker list
    WORKERS = [("worker-" + str(i+1), 18861) for i in range(num_workers)]
    
    # DOWNLOAD AND UNZIP DATASET
    url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik9.zip'
    
    print("="*60)
    print("DISTRIBUTED MAPREDUCE WORD COUNT")
    print("="*60)
    print(f"Workers: {num_workers}")
    print(f"Reduce Tasks: {num_reduce_tasks}")
    print(f"Dataset: {url}")
    print("="*60 + "\n")
    
    input_files = download(url)
    
    # Create coordinator instance
    coordinator = Coordinator()
    coordinator.num_reduce_tasks = num_reduce_tasks
    _coordinator_instance = coordinator
    
    # Create map tasks
    print("\n[INIT] Creating map tasks...")
    chunks = split_files_into_chunks(input_files, num_workers * 2)  # 2x workers for better load balancing
    for i, chunk in enumerate(chunks):
        task = Task(TaskType.MAP, coordinator.next_task_id, chunk)
        coordinator.map_tasks.append(task)
        coordinator.next_task_id += 1
    
    print(f"[INIT] Created {len(coordinator.map_tasks)} map tasks")
    
    # Create reduce tasks (will be populated after map phase)
    print(f"[INIT] Will create {num_reduce_tasks} reduce tasks after map phase")
    
    # Start coordinator RPC server
    from rpyc.utils.server import ThreadedServer
    
    coordinator_server = ThreadedServer(
        CoordinatorService,
        port=18862,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle": True,
            "sync_request_timeout": 300  # Increased timeout for large data transfers
        }
    )
    
    # Start coordinator server in background thread
    def run_coordinator():
        coordinator_server.start()
    
    coordinator_thread = threading.Thread(target=run_coordinator, daemon=True)
    coordinator_thread.start()
    
    print("[INIT] Coordinator RPC server started on port 18862")
    
    # Wait a bit for server to fully initialize
    time.sleep(2)
    
    # Wait for workers to start and connect to coordinator
    print("\n[INIT] Waiting for workers to connect...")
    print("[INIT] Workers will connect as they start up...")
    
    # Wait and check for worker connections
    max_wait = 30  # Maximum 30 seconds
    check_interval = 2
    waited = 0
    
    while waited < max_wait:
        time.sleep(check_interval)
        waited += check_interval
        with coordinator.lock:
            connected_count = len(coordinator.connected_workers)
            print(f"[INIT] Connected workers: {connected_count}/{num_workers} (waited {waited}s)")
            
            # If we have at least one worker, we can proceed (others can join later)
            if connected_count > 0:
                print(f"[INIT] At least one worker connected, proceeding...")
                break
    
    # Final check
    with coordinator.lock:
        final_count = len(coordinator.connected_workers)
        if final_count == 0:
            print("\n[WARNING] No workers connected! MapReduce may not work.")
            print("[WARNING] Check worker logs to see connection errors.")
            print("[WARNING] Proceeding anyway, but tasks may not complete...")
        else:
            print(f"[INIT] {final_count} worker(s) connected and ready")
    
    print("\n" + "="*60)
    print("STARTING MAPREDUCE")
    print("="*60)
    
    start_time = time.time()
    
    # Wait for map phase to complete
    print("\n[MAP] Waiting for map tasks to complete...")
    map_start = time.time()
    last_progress = -1
    no_progress_count = 0
    
    while not all(t.completed for t in coordinator.map_tasks):
        time.sleep(1)
        completed = sum(1 for t in coordinator.map_tasks if t.completed)
        total = len(coordinator.map_tasks)
        
        # Check if progress is stuck
        if completed == last_progress:
            no_progress_count += 1
        else:
            no_progress_count = 0
            last_progress = completed
        
        # Print progress every 5 tasks or every 10 seconds if stuck
        if completed % 5 == 0 or completed == total or no_progress_count % 10 == 0:
            with coordinator.lock:
                active_workers = len(coordinator.connected_workers)
                assigned = sum(1 for t in coordinator.map_tasks if t.assigned_to is not None)
            print(f"  Map progress: {completed}/{total} tasks completed | {assigned} assigned | {active_workers} workers connected")
        
        # Warn if stuck for too long
        if no_progress_count > 30:
            print(f"\n[WARNING] No progress for 30 seconds! Check worker logs.")
            print(f"[WARNING] Connected workers: {len(coordinator.connected_workers)}")
            print(f"[WARNING] Assigned tasks: {sum(1 for t in coordinator.map_tasks if t.assigned_to is not None)}")
            no_progress_count = 0  # Reset counter
    
    map_time = time.time() - map_start
    print(f"[MAP] Map phase complete! Time: {map_time:.2f}s")
    
    # Create reduce tasks based on partitions
    print("\n[REDUCE] Creating reduce tasks...")
    for partition in range(num_reduce_tasks):
        intermediate_data = coordinator.get_intermediate_data(partition)
        if intermediate_data:  # Only create task if there's data
            task = Task(TaskType.REDUCE, coordinator.next_task_id, intermediate_data)
            coordinator.reduce_tasks.append(task)
            coordinator.next_task_id += 1
        else:
            # Still create task with empty data to maintain partition count
            task = Task(TaskType.REDUCE, coordinator.next_task_id, {})
            coordinator.reduce_tasks.append(task)
            coordinator.next_task_id += 1
    
    print(f"[REDUCE] Created {len(coordinator.reduce_tasks)} reduce tasks")
    
    # Wait for reduce phase to complete
    print("[REDUCE] Waiting for reduce tasks to complete...")
    reduce_start = time.time()
    while not all(t.completed for t in coordinator.reduce_tasks):
        time.sleep(1)
        completed = sum(1 for t in coordinator.reduce_tasks if t.completed)
        total = len(coordinator.reduce_tasks)
        if completed % 2 == 0 or completed == total:
            print(f"  Reduce progress: {completed}/{total} tasks completed")
    
    reduce_time = time.time() - reduce_start
    print(f"[REDUCE] Reduce phase complete! Time: {reduce_time:.2f}s")
    
    # Get final results
    print("\n[AGGREGATE] Aggregating final results...")
    final_counts = coordinator.get_final_results()
    total_counts = sorted(final_counts.items(), key=operator.itemgetter(1), reverse=True)
    
    end_time = time.time()
    
    # Stop coordinator server
    print("[CLEANUP] Shutting down coordinator server...")
    coordinator_server.close()
    
    print('\n' + '='*60)
    print('TOP 20 WORDS BY FREQUENCY')
    print('='*60 + '\n')
    
    top20 = total_counts[0:20]
    longest = max(len(word) for word, count in top20) if top20 else 5
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1
    
    elapsed_time = end_time - start_time
    print("\n" + "="*60)
    print("Elapsed Time: {:.2f} seconds".format(elapsed_time))
    print("="*60)
