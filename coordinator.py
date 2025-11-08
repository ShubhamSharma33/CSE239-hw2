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
    # ("worker-2", 18861),
    # ("worker-3", 18861),
    # ("worker-4", 18861),
]

CHUNK_SIZE = 1024 * 1024 * 50  # 50MB chunks
MAX_PENDING = 15  # Max async operations at once

def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    """Download and extract dataset"""
    filename = url.split('/')[-1]
    
    if os.path.exists('txt/') and len(glob.glob('txt/*')) > 0:
        print(f"[INFO] Dataset already exists")
        return glob.glob('txt/*')
    
    print(f"[INFO] Downloading {filename}...")
    urllib.request.urlretrieve(url, filename)
    
    print(f"[INFO] Extracting...")
    os.makedirs('txt', exist_ok=True)
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall('txt/')
    
    return glob.glob('txt/*')

def stream_chunks(filepath, chunk_bytes=CHUNK_SIZE):
    """Stream file in chunks - memory efficient"""
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        buffer = []
        size = 0
        
        for line in f:
            buffer.append(line)
            size += len(line)
            
            if size >= chunk_bytes:
                yield ''.join(buffer)
                buffer = []
                size = 0
        
        if buffer:
            yield ''.join(buffer)

def mapreduce_wordcount(input_files, workers):
    """Fast MapReduce with async RPyC"""
    
    print(f"\n[MAPREDUCE] Starting with {len(workers)} workers")
    
    # Connect to all workers upfront
    connections = []
    for hostname, port in workers:
        conn = rpyc.connect(
            hostname, 
            port,
            config={
                "allow_public_attrs": True,
                "allow_pickle": True,
                "sync_request_timeout": 120
            }
        )
        connections.append(conn)
        print(f"[WORKER] Connected to {hostname}")
    
    # Track results
    word_totals = defaultdict(int)
    pending = []
    worker_idx = 0
    chunks_sent = 0
    
    def collect_ready():
        """Collect completed async results"""
        nonlocal pending, word_totals
        still_pending = []
        
        for async_res in pending:
            if async_res.ready:
                result = async_res.value  # dict of word:count
                for word, count in result.items():
                    word_totals[word] += count
            else:
                still_pending.append(async_res)
        
        pending = still_pending
    
    # Process all files
    print(f"[MAP] Processing {len(input_files)} file(s)...")
    
    for filepath in input_files:
        print(f"[MAP] Reading {filepath}")
        
        for chunk in stream_chunks(filepath):
            # Wait if too many pending
            while len(pending) >= MAX_PENDING:
                collect_ready()
                time.sleep(0.02)
            
            # Send async to next worker
            conn = connections[worker_idx]
            async_result = rpyc.async_(conn.root.process_chunk)(chunk)
            pending.append(async_result)
            
            worker_idx = (worker_idx + 1) % len(connections)
            chunks_sent += 1
            
            if chunks_sent % 5 == 0:
                print(f"[MAP] Sent {chunks_sent} chunks, {len(pending)} pending")
    
    # Wait for all remaining
    print(f"[MAP] Waiting for {len(pending)} remaining tasks...")
    while pending:
        collect_ready()
        time.sleep(0.05)
    
    # Close connections
    for conn in connections:
        conn.close()
    
    print(f"[COMPLETE] Processed {chunks_sent} chunks")
    print(f"[COMPLETE] Found {len(word_totals)} unique words")
    
    # Sort by frequency
    sorted_results = sorted(word_totals.items(), key=itemgetter(1), reverse=True)
    return sorted_results

if __name__ == "__main__":
    print("="*60)
    print("DISTRIBUTED MAPREDUCE WORD COUNT")
    print("="*60)
    
    # Download
    files = download()
    
    # Wait for workers
    print("\n[INIT] Waiting for workers to start...")
    time.sleep(5)
    
    # Run MapReduce
    start = time.time()
    results = mapreduce_wordcount(files, WORKERS)
    end = time.time()
    
    # Display results
    print("\n" + "="*60)
    print("TOP 20 WORDS")
    print("="*60)
    
    for i, (word, count) in enumerate(results[:20], 1):
        print(f"{i:2d}. {word:15s} {count:>10,}")
    
    # Save results
    with open('txt/output.txt', 'w') as f:
        for word, count in results:
            f.write(f"{word}\t{count}\n")
    
    print("="*60)
    print(f"ELAPSED TIME: {end - start:.2f} seconds")
    print(f"Saved {len(results):,} words to txt/output.txt")
    print("="*60)