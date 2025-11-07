#!/usr/bin/env python3
"""
MapReduce Worker - Polling Architecture
"""

import os
import time
import rpyc
import string
from collections import Counter

COORDINATOR_HOST = os.environ.get("COORDINATOR_HOST", "coordinator")
COORDINATOR_PORT = 18860
WORKER_ID = os.environ.get("HOSTNAME", "worker-unknown")
POLL_INTERVAL = 0.5


def tokenize_file(file_path):
    word_counts = Counter()
    translator = str.maketrans('', '', string.punctuation + string.digits)
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                cleaned = line.translate(translator).lower()
                words = cleaned.split()
                words = [w for w in words if w.isalpha() and len(w) > 1]
                word_counts.update(words)
        
        print(f"[{WORKER_ID}] Processed {file_path}: {sum(word_counts.values())} words, {len(word_counts)} unique")
        return dict(word_counts)
    
    except Exception as e:
        print(f"[{WORKER_ID}] Error processing {file_path}: {e}")
        return {}


def execute_map_task(file_path):
    return tokenize_file(file_path)


def execute_reduce_task(partition_id, grouped_data):
    reduced = {}
    for word, counts in grouped_data.items():
        reduced[word] = sum(counts)
    
    print(f"[{WORKER_ID}] Reduced partition {partition_id}: {len(reduced)} words, total count={sum(reduced.values())}")
    return reduced


def worker_loop():
    print(f"[{WORKER_ID}] Starting worker loop...")
    print(f"[{WORKER_ID}] Connecting to coordinator at {COORDINATOR_HOST}:{COORDINATOR_PORT}")
    
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = rpyc.connect(
                COORDINATOR_HOST,
                COORDINATOR_PORT,
                config={"allow_public_attrs": True, "allow_pickle": True, "sync_request_timeout": 60}
            )
            print(f"[{WORKER_ID}] Connected to coordinator!")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[{WORKER_ID}] Connection attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
            else:
                print(f"[{WORKER_ID}] Failed to connect to coordinator: {e}")
                return
    
    consecutive_empty = 0
    max_empty = 20
    
    while True:
        try:
            task = conn.root.get_task(WORKER_ID)
            
            if task is None:
                consecutive_empty += 1
                
                if conn.root.is_all_done():
                    print(f"[{WORKER_ID}] All work complete, shutting down.")
                    break
                
                if conn.root.is_map_done():
                    try:
                        conn.root.trigger_shuffle()
                    except:
                        pass
                
                if consecutive_empty >= max_empty:
                    print(f"[{WORKER_ID}] No tasks available for {max_empty * POLL_INTERVAL}s, checking if done...")
                    if conn.root.is_all_done():
                        print(f"[{WORKER_ID}] Work complete, exiting.")
                        break
                    consecutive_empty = 0
                
                time.sleep(POLL_INTERVAL)
                continue
            
            consecutive_empty = 0
            task_id = task['task_id']
            task_type = task['task_type']
            data = task['data']
            
            print(f"[{WORKER_ID}] Received task: {task_id} (type={task_type})")
            
            if task_type == 'map':
                result = execute_map_task(data)
            elif task_type == 'reduce':
                partition_id, grouped_data = data
                result = execute_reduce_task(partition_id, grouped_data)
            else:
                print(f"[{WORKER_ID}] Unknown task type: {task_type}")
                continue
            
            success = conn.root.submit_result(task_id, result, WORKER_ID)
            if success:
                print(f"[{WORKER_ID}] Task {task_id} completed successfully")
            else:
                print(f"[{WORKER_ID}] Task {task_id} submission rejected (may have timed out)")
        
        except Exception as e:
            print(f"[{WORKER_ID}] Error in worker loop: {e}")
            time.sleep(1)
    
    conn.close()
    print(f"[{WORKER_ID}] Worker shutting down.")


if __name__ == "__main__":
    print(f"[{WORKER_ID}] Worker starting...")
    print(f"[{WORKER_ID}] Environment: COORDINATOR_HOST={COORDINATOR_HOST}")
    
    time.sleep(2)
    
    worker_loop()