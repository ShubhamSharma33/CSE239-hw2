#!/usr/bin/env python3
"""
MapReduce Coordinator with Task Queue and Fault Tolerance
"""

import os
import sys
import time
import glob
import urllib.request
import zipfile
import threading
from collections import defaultdict, Counter
from enum import Enum
import hashlib


class TaskType(Enum):
    MAP = "map"
    REDUCE = "reduce"


class TaskState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Task:
    def __init__(self, task_id, task_type, data):
        self.task_id = task_id
        self.task_type = task_type
        self.data = data
        self.state = TaskState.PENDING
        self.assigned_worker = None
        self.start_time = None
        self.result = None
        self.attempts = 0

    def assign(self, worker_id):
        self.state = TaskState.RUNNING
        self.assigned_worker = worker_id
        self.start_time = time.time()
        self.attempts += 1

    def complete(self, result):
        self.state = TaskState.COMPLETED
        self.result = result

    def fail(self):
        self.state = TaskState.FAILED
        self.assigned_worker = None
        self.start_time = None

    def is_timeout(self, timeout=20):
        if self.state == TaskState.RUNNING and self.start_time:
            return time.time() - self.start_time > timeout
        return False


class TaskQueue:
    def __init__(self, num_reduce_partitions=3):
        self.tasks = {}
        self.lock = threading.Lock()
        self.num_reduce_partitions = num_reduce_partitions
        self.map_results = []
        self.task_counter = 0

    def add_map_task(self, file_path):
        with self.lock:
            task_id = f"map_{self.task_counter}"
            self.task_counter += 1
            task = Task(task_id, TaskType.MAP, file_path)
            self.tasks[task_id] = task
            return task_id

    def add_reduce_task(self, partition_id, grouped_data):
        with self.lock:
            task_id = f"reduce_{partition_id}"
            task = Task(task_id, TaskType.REDUCE, (partition_id, grouped_data))
            self.tasks[task_id] = task
            return task_id

    def get_pending_task(self, worker_id):
        with self.lock:
            for task in self.tasks.values():
                if task.is_timeout():
                    print(f"[COORD] Task {task.task_id} timed out, reassigning from {task.assigned_worker} to {worker_id}")
                    task.fail()
                    task.state = TaskState.PENDING

            for task in self.tasks.values():
                if task.state == TaskState.PENDING:
                    task.assign(worker_id)
                    return {
                        'task_id': task.task_id,
                        'task_type': task.task_type.value,
                        'data': task.data
                    }
            return None

    def complete_task(self, task_id, result, worker_id):
        with self.lock:
            if task_id not in self.tasks:
                return False
            
            task = self.tasks[task_id]
            if task.state != TaskState.RUNNING or task.assigned_worker != worker_id:
                print(f"[COORD] Ignoring stale result for {task_id} from {worker_id}")
                return False

            task.complete(result)
            
            if task.task_type == TaskType.MAP:
                self.map_results.append(result)
            
            return True

    def all_tasks_done(self, task_type=None):
        with self.lock:
            for task in self.tasks.values():
                if task_type and task.task_type != task_type:
                    continue
                if task.state != TaskState.COMPLETED:
                    return False
            return True

    def get_status(self):
        with self.lock:
            status = {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0}
            for task in self.tasks.values():
                status[task.state.value] += 1
            return status


def download_dataset(url='https://mattmahoney.net/dc/enwik9.zip'):
    os.makedirs('txt', exist_ok=True)
    filename = url.split('/')[-1]
    zip_path = filename

    if not os.path.exists(zip_path):
        print(f"[COORD] Downloading {url}...")
        urllib.request.urlretrieve(url, zip_path)
        print("[COORD] Download complete.")
    else:
        print(f"[COORD] Using cached {zip_path}")

    existing = [p for p in glob.glob('txt/*') if os.path.isfile(p)]
    if not existing:
        print(f"[COORD] Extracting {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall('txt/')
        print("[COORD] Extraction complete.")
    else:
        print("[COORD] txt/ already populated, skipping extraction.")

    files = sorted(p for p in glob.glob('txt/*') if os.path.isfile(p))
    print(f"[COORD] Found {len(files)} files.")
    return files


def partition_key(word, num_partitions):
    return int(hashlib.md5(word.encode()).hexdigest(), 16) % num_partitions


def shuffle_phase(map_results, num_partitions):
    print(f"[COORD] Starting shuffle phase with {num_partitions} partitions...")
    
    partitions = [defaultdict(list) for _ in range(num_partitions)]
    
    for map_result in map_results:
        for word, count in map_result.items():
            pid = partition_key(word, num_partitions)
            partitions[pid][word].append(count)
    
    partitions = [dict(p) for p in partitions]
    
    total_keys = sum(len(p) for p in partitions)
    print(f"[COORD] Shuffle complete: {total_keys} unique words across {num_partitions} partitions")
    
    return partitions


GLOBAL_QUEUE = None


def initialize_coordinator(input_files, num_reduce_partitions):
    global GLOBAL_QUEUE
    GLOBAL_QUEUE = TaskQueue(num_reduce_partitions)
    
    print(f"[COORD] Initializing with {len(input_files)} map tasks...")
    for file_path in input_files:
        GLOBAL_QUEUE.add_map_task(file_path)


def handle_get_task(worker_id):
    if GLOBAL_QUEUE is None:
        return None
    return GLOBAL_QUEUE.get_pending_task(worker_id)


def handle_submit_result(task_id, result, worker_id):
    if GLOBAL_QUEUE is None:
        return False
    return GLOBAL_QUEUE.complete_task(task_id, result, worker_id)


def is_map_phase_done():
    if GLOBAL_QUEUE is None:
        return False
    return GLOBAL_QUEUE.all_tasks_done(TaskType.MAP)


def is_all_done():
    if GLOBAL_QUEUE is None:
        return False
    return GLOBAL_QUEUE.all_tasks_done()


def get_shuffle_data():
    if GLOBAL_QUEUE is None:
        return []
    
    partitions = shuffle_phase(GLOBAL_QUEUE.map_results, GLOBAL_QUEUE.num_reduce_partitions)
    
    for pid, grouped_data in enumerate(partitions):
        GLOBAL_QUEUE.add_reduce_task(pid, grouped_data)
    
    return len(partitions)


def get_final_results():
    if GLOBAL_QUEUE is None:
        return {}
    
    final_counts = Counter()
    for task in GLOBAL_QUEUE.tasks.values():
        if task.task_type == TaskType.REDUCE and task.state == TaskState.COMPLETED:
            final_counts.update(task.result)
    
    return dict(final_counts)


import rpyc
from rpyc.utils.server import ThreadedServer


class CoordinatorService(rpyc.Service):
    def exposed_get_task(self, worker_id):
        return handle_get_task(worker_id)
    
    def exposed_submit_result(self, task_id, result, worker_id):
        return handle_submit_result(task_id, result, worker_id)
    
    def exposed_is_map_done(self):
        return is_map_phase_done()
    
    def exposed_trigger_shuffle(self):
        return get_shuffle_data()
    
    def exposed_is_all_done(self):
        return is_all_done()
    
    def exposed_get_results(self):
        return get_final_results()


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik9.zip'
    num_workers = int(os.environ.get("NUM_WORKERS", "3"))
    
    print(f"[COORD] Starting coordinator with {num_workers} workers")
    
    files = download_dataset(url)
    initialize_coordinator(files, num_reduce_partitions=num_workers)
    
    print("[COORD] Starting RPC server on port 18860...")
    server = ThreadedServer(
        CoordinatorService,
        hostname="0.0.0.0",
        port=18860,
        protocol_config={"allow_public_attrs": True, "allow_pickle": True},
    )
    
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    print("[COORD] Waiting 3 seconds for workers to connect...")
    time.sleep(3)
    
    start_time = time.time()
    
    while not is_map_phase_done():
        status = GLOBAL_QUEUE.get_status()
        print(f"[COORD] Map phase: {status}")
        time.sleep(2)
    
    print("[COORD] Map phase complete, triggering shuffle...")
    get_shuffle_data()
    
    while not is_all_done():
        status = GLOBAL_QUEUE.get_status()
        print(f"[COORD] Reduce phase: {status}")
        time.sleep(2)
    
    end_time = time.time()
    
    print("[COORD] All tasks complete, aggregating results...")
    final_counts = get_final_results()
    
    print("\n" + "="*60)
    print("TOP 20 WORDS BY FREQUENCY")
    print("="*60 + "\n")
    
    sorted_counts = sorted(final_counts.items(), key=lambda x: x[1], reverse=True)
    top20 = sorted_counts[:20]
    longest = max(len(w) for w, _ in top20) if top20 else 5
    
    for i, (w, c) in enumerate(top20, 1):
        print(f"{i:2d}. {w:{longest+1}s}: {c:,}")
    
    out_path = "txt/word_counts.tsv"
    with open(out_path, "w", encoding="utf-8") as f:
        for w, c in sorted_counts:
            f.write(f"{w}\t{c}\n")
    
    print(f"\nSaved: {out_path} (unique={len(final_counts)})")
    print(f"Elapsed Time: {end_time - start_time:.2f} seconds")
    
    server.close()
    print("[COORD] Coordinator shutting down.")


if __name__ == "__main__":
    main()