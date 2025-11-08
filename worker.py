import rpyc
from collections import Counter
import re
import time
import os
import socket

class WorkerService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """Map step: tokenize and count words in text chunk."""
        # Extract words
        words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
        
        # Count frequencies
        counts = Counter(words)
        
        # Return as plain dict (important for RPyC)
        return dict(counts)
    
    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""
        # grouped_items is a dict of {word: count}
        # Just return it as-is since aggregation happens in coordinator
        # In a more complex scenario, this could do additional processing
        return grouped_items

def get_worker_id():
    """Get worker ID from hostname (e.g., worker-1 -> 1)"""
    hostname = socket.gethostname()
    if hostname.startswith('worker-'):
        return hostname
    return f"worker-{os.environ.get('WORKER_ID', 'unknown')}"

def worker_main():
    """Main worker loop: request tasks from coordinator and execute them."""
    worker_id = get_worker_id()
    coordinator_host = os.environ.get('COORDINATOR_HOST', 'coordinator')
    coordinator_port = int(os.environ.get('COORDINATOR_PORT', '18862'))
    
    print(f"[{worker_id}] Starting worker...")
    print(f"[{worker_id}] Coordinator: {coordinator_host}:{coordinator_port}")
    
    # Connect to coordinator
    max_retries = 20  # Increased retries
    retry_delay = 2
    coordinator_conn = None
    
    for attempt in range(max_retries):
        try:
            print(f"[{worker_id}] Connecting to coordinator (attempt {attempt + 1}/{max_retries})...")
            coordinator_conn = rpyc.connect(
                coordinator_host,
                coordinator_port,
                config={
                    "allow_public_attrs": True,
                    "allow_pickle": True,
                    "sync_request_timeout": 60  # Increased timeout
                }
            )
            print(f"[{worker_id}] Connected to coordinator!")
            break
        except ConnectionRefusedError as e:
            if attempt < max_retries - 1:
                print(f"[{worker_id}] Connection refused (coordinator not ready), retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"[{worker_id}] Failed to connect to coordinator after {max_retries} attempts")
                return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[{worker_id}] Connection failed: {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"[{worker_id}] Failed to connect to coordinator after {max_retries} attempts: {e}")
                return
    
    # Worker loop: request tasks and execute them
    print(f"[{worker_id}] Entering task loop...")
    
    while True:
        try:
            # Request a task from coordinator
            task = coordinator_conn.root.request_task(worker_id)
            
            if task is None:
                # No tasks available, wait a bit
                time.sleep(1)
                continue
            
            if not isinstance(task, dict):
                print(f"[{worker_id}] Invalid task format: {type(task)}")
                time.sleep(1)
                continue
            
            if task.get("task_type") == "done":
                print(f"[{worker_id}] All tasks completed, exiting...")
                break
            
            task_type = task.get("task_type")
            task_id = task.get("task_id")
            task_data = task.get("data")
            
            if task_type == "map":
                print(f"[{worker_id}] Received MAP task {task_id}")
                # Execute map function
                result = map_function(task_data)
                # Submit result
                try:
                    success = coordinator_conn.root.submit_map_result(task_id, result, worker_id)
                    if success:
                        print(f"[{worker_id}] Completed MAP task {task_id}")
                    else:
                        print(f"[{worker_id}] Failed to submit MAP task {task_id} result")
                except Exception as e:
                    print(f"[{worker_id}] Error submitting map result: {e}")
            
            elif task_type == "reduce":
                print(f"[{worker_id}] Received REDUCE task {task_id}")
                # Execute reduce function
                result = reduce_function(task_data)
                # Submit result
                try:
                    success = coordinator_conn.root.submit_reduce_result(task_id, result, worker_id)
                    if success:
                        print(f"[{worker_id}] Completed REDUCE task {task_id}")
                    else:
                        print(f"[{worker_id}] Failed to submit REDUCE task {task_id} result")
                except Exception as e:
                    print(f"[{worker_id}] Error submitting reduce result: {e}")
            
            else:
                print(f"[{worker_id}] Unknown task type: {task_type}")
        
        except (ConnectionError, EOFError, OSError) as e:
            print(f"[{worker_id}] Connection error in task loop: {e}, reconnecting...")
            # Try to reconnect
            try:
                coordinator_conn.close()
            except:
                pass
            time.sleep(5)
            # Reconnect
            try:
                coordinator_conn = rpyc.connect(
                    coordinator_host,
                    coordinator_port,
                    config={
                        "allow_public_attrs": True,
                        "allow_pickle": True,
                        "sync_request_timeout": 60
                    }
                )
                print(f"[{worker_id}] Reconnected to coordinator")
            except Exception as reconnect_error:
                print(f"[{worker_id}] Failed to reconnect: {reconnect_error}")
                time.sleep(5)
        except Exception as e:
            print(f"[{worker_id}] Error in task loop: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(2)
    
    coordinator_conn.close()
    print(f"[{worker_id}] Worker shutting down...")

def map_function(text_chunk):
    """Map function: tokenize and count words."""
    words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
    counts = Counter(words)
    return dict(counts)

def reduce_function(grouped_items):
    """Reduce function: aggregate counts (already grouped by coordinator)."""
    # grouped_items is a dict of {word: count}
    # In this simple case, it's already aggregated, but we could do more processing here
    return grouped_items

if __name__ == "__main__":
    # Start RPyC server for direct calls (if needed for compatibility)
    from rpyc.utils.server import ThreadedServer
    
    # Also start worker main loop in a thread
    import threading
    
    worker_thread = threading.Thread(target=worker_main, daemon=False)
    worker_thread.start()
    
    # Start RPC server for backward compatibility
    print("Worker RPC server starting on port 18861...")
    server = ThreadedServer(
        WorkerService,
        port=18861,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle": True,
            "sync_request_timeout": 120
        }
    )
    
    # Run server in background thread
    def run_server():
        server.start()
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait for worker thread to complete
    worker_thread.join()
    
    print("Worker process exiting...")
