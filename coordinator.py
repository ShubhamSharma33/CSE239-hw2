import rpyc
import time
import urllib.request
import zipfile
import os
import glob
from collections import defaultdict
from operator import itemgetter

# Configuration
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

# Streaming configuration - don't load entire file!
STREAM_CHUNK_SIZE = 1024 * 1024 * 50  # 50MB chunks

def download(url='https://mattmahoney.net/dc/enwik8.zip'):
    """Download and extract dataset"""
    filename = url.split('/')[-1]
    
    if os.path.exists('txt/') and len(glob.glob('txt/*')) > 0:
        print(f"Dataset already exists, skipping download")
        return glob.glob('txt/*')
    
    print(f"Downloading {filename}...")
    urllib.request.urlretrieve(url, filename)
    
    print(f"Extracting {filename}...")
    os.makedirs('txt', exist_ok=True)
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall('txt/')
    
    return glob.glob('txt/*')

def stream_file_in_chunks(filepath, chunk_size=STREAM_CHUNK_SIZE):
    """
    Generator that yields text chunks without loading entire file.
    This is KEY to handling large files efficiently!
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        buffer = []
        buffer_size = 0
        
        for line in f:
            buffer.append(line)
            buffer_size += len(line)
            
            # When buffer reaches chunk size, yield it
            if buffer_size >= chunk_size:
                yield ''.join(buffer)
                buffer = []
                buffer_size = 0
        
        # Don't forget remaining data
        if buffer:
            yield ''.join(buffer)

class WorkerPool:
    """Manages persistent connections to workers with round-robin distribution"""
    
    def __init__(self, worker_addresses):
        self.workers = worker_addresses
        self.connections = []
        self.next_worker_idx = 0
        
        # Establish all connections upfront
        print(f"Connecting to {len(worker_addresses)} workers...")
        for hostname, port in worker_addresses:
            try:
                conn = rpyc.connect(
                    hostname, 
                    port,
                    config={
                        "allow_public_attrs": True,
                        "sync_request_timeout": 90
                    }
                )
                self.connections.append(conn)
                print(f"  ✓ Connected to {hostname}:{port}")
            except Exception as e:
                print(f"  ✗ Failed to connect to {hostname}:{port} - {e}")
                raise
    
    def get_next_connection(self):
        """Round-robin worker selection"""
        conn = self.connections[self.next_worker_idx]
        self.next_worker_idx = (self.next_worker_idx + 1) % len(self.connections)
        return conn
    
    def close_all(self):
        """Clean up all connections"""
        for conn in self.connections:
            try:
                conn.close()
            except:
                pass

def mapreduce_wordcount(input_files, worker_pool):
    """
    Optimized MapReduce using:
    1. Async RPyC calls (non-blocking)
    2. Streaming file reading (low memory)
    3. Persistent connections (no reconnect overhead)
    """
    
    print("\n" + "="*60)
    print("STARTING MAP PHASE WITH ASYNC CALLS")
    print("="*60)
    
    # Track async operations
    pending_tasks = []
    all_word_counts = defaultdict(int)
    
    def collect_completed_results():
        """Check which async tasks are done and aggregate their results"""
        nonlocal pending_tasks, all_word_counts
        
        still_pending = []
        for async_result in pending_tasks:
            if async_result.ready:
                # Get result and aggregate
                word_dict = async_result.value
                for word, count in word_dict.items():
                    all_word_counts[word] += count
            else:
                still_pending.append(async_result)
        
        pending_tasks = still_pending
    
    # Process each file
    total_chunks_sent = 0
    map_start = time.time()
    
    for filepath in input_files:
        print(f"\nProcessing file: {filepath}")
        
        # Stream file in chunks
        for chunk_text in stream_file_in_chunks(filepath):
            # Get next available worker connection
            worker_conn = worker_pool.get_next_connection()
            
            # Send async map request (non-blocking!)
            async_result = rpyc.async_(worker_conn.root.exposed_map)(chunk_text)
            pending_tasks.append(async_result)
            total_chunks_sent += 1
            
            # Periodically collect completed results to avoid memory buildup
            if len(pending_tasks) > 20:
                collect_completed_results()
                time.sleep(0.01)  # Small delay to let tasks complete
            
            if total_chunks_sent % 10 == 0:
                print(f"  Dispatched {total_chunks_sent} chunks, {len(pending_tasks)} pending...")
    
    # Wait for all remaining tasks to complete
    print(f"\nWaiting for {len(pending_tasks)} remaining tasks...")
    while pending_tasks:
        collect_completed_results()
        if pending_tasks:
            time.sleep(0.1)
    
    map_time = time.time() - map_start
    print(f"\n✓ Map phase complete!")
    print(f"  Total chunks processed: {total_chunks_sent}")
    print(f"  Unique words found: {len(all_word_counts)}")
    print(f"  Time taken: {map_time:.2f} seconds")
    
    # No separate reduce phase needed - we aggregated during map!
    # Sort results by frequency
    sorted_results = sorted(all_word_counts.items(), key=itemgetter(1), reverse=True)
    
    return sorted_results

if __name__ == "__main__":
    print("="*60)
    print("MEMORY-OPTIMIZED MAPREDUCE WORD COUNT")
    print("Using: Async RPyC + Streaming Files + Persistent Connections")
    print("="*60)
    
    # Download dataset
    input_files = download()
    
    # Wait for workers to be ready
    print("\nWaiting for workers to start...")
    time.sleep(5)
    
    # Create worker pool with persistent connections
    worker_pool = WorkerPool(WORKERS)
    
    try:
        # Run MapReduce
        overall_start = time.time()
        word_counts = mapreduce_wordcount(input_files, worker_pool)
        overall_end = time.time()
        
        # Display results
        print('\n' + '='*60)
        print('TOP 20 MOST FREQUENT WORDS')
        print('='*60)
        
        top20 = word_counts[:20]
        max_word_len = max(len(word) for word, count in top20) if top20 else 10
        
        for rank, (word, count) in enumerate(top20, 1):
            print(f'{rank:2d}. {word:<{max_word_len+2}} {count:>10,}')
        
        # Save full results
        output_file = 'txt/results.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            for word, count in word_counts:
                f.write(f'{word}\t{count}\n')
        
        print('\n' + '='*60)
        print(f'✓ Complete! Saved results to {output_file}')
        print(f'✓ Total unique words: {len(word_counts):,}')
        print(f'✓ TOTAL ELAPSED TIME: {overall_end - overall_start:.2f} seconds')
        print('='*60)
        
    finally:
        # Always clean up connections
        worker_pool.close_all()