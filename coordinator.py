import rpyc
import time
import urllib.request
import zipfile
import os
import glob
from collections import defaultdict
from operator import itemgetter
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
    ("worker-3", 18861),
    ("worker-4", 18861),
]

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

def split_text(files, n_workers):
    """Split files into n_workers chunks"""
    all_text = ""
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            all_text += f.read()
    
    chunk_size = len(all_text) // n_workers
    chunks = []
    
    for i in range(n_workers):
        start = i * chunk_size
        end = start + chunk_size if i < n_workers - 1 else len(all_text)
        chunks.append(all_text[start:end])
    
    return chunks

def map_worker(worker_info, chunk):
    """Execute map on a single worker - for parallel execution"""
    hostname, port = worker_info
    try:
        conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
        result = conn.root.exposed_map(chunk)
        
        # CRITICAL: Convert netref to actual list BEFORE closing connection!
        result = list(result)
        
        conn.close()
        return (hostname, result, None)
    except Exception as e:
        return (hostname, None, str(e))

def reduce_worker(worker_info, word_groups):
    """Execute reduce on a single worker - for parallel execution"""
    hostname, port = worker_info
    results = {}
    try:
        conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
        
        # Process ALL words assigned to this worker in one connection
        for word, pairs in word_groups.items():
            total = conn.root.exposed_reduce(pairs)
            # CRITICAL: Convert to int BEFORE closing connection!
            results[word] = int(total)
        
        conn.close()
        return (hostname, results, None)
    except Exception as e:
        return (hostname, None, str(e))

def mapreduce_wordcount(input_files):
    n_workers = len(WORKERS)
    
    # 1. Split text into chunks
    print(f"Splitting data into {n_workers} chunks...")
    chunks = split_text(input_files, n_workers)
    
    # 2. MAP PHASE - PARALLEL!
    print("MAP PHASE: Sending chunks to workers IN PARALLEL...")
    map_start = time.time()
    
    map_results = []
    
    # Use ThreadPoolExecutor for parallel map operations
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(map_worker, WORKERS[i], chunks[i]): i 
            for i in range(n_workers)
        }
        
        for future in as_completed(futures):
            hostname, result, error = future.result()
            if error:
                print(f"  ✗ {hostname} failed: {error}")
            else:
                map_results.extend(result)
                print(f"  ✓ {hostname} completed")
    
    map_time = time.time() - map_start
    print(f"  Map phase took: {map_time:.2f} seconds")
    
    # 3. SHUFFLE PHASE
    print("SHUFFLE PHASE: Grouping intermediate results...")
    shuffle_start = time.time()
    
    grouped = defaultdict(list)
    for word, count in map_results:
        grouped[word].append((word, count))
    
    shuffle_time = time.time() - shuffle_start
    print(f"  Shuffle phase took: {shuffle_time:.2f} seconds")
    
    # 4. REDUCE PHASE - PARALLEL with BATCHING!
    print("REDUCE PHASE: Aggregating counts IN PARALLEL...")
    reduce_start = time.time()
    
    # Partition words across workers
    words = list(grouped.keys())
    words_per_worker = len(words) // n_workers
    
    # Create batches for each worker
    worker_batches = []
    for i in range(n_workers):
        start_idx = i * words_per_worker
        end_idx = start_idx + words_per_worker if i < n_workers - 1 else len(words)
        worker_words = words[start_idx:end_idx]
        
        # Create a dict of word -> pairs for this worker
        batch = {word: grouped[word] for word in worker_words}
        worker_batches.append(batch)
    
    final_counts = {}
    
    # Use ThreadPoolExecutor for parallel reduce operations
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(reduce_worker, WORKERS[i], worker_batches[i]): i 
            for i in range(n_workers)
        }
        
        for future in as_completed(futures):
            hostname, results, error = future.result()
            if error:
                print(f"  ✗ {hostname} failed: {error}")
            else:
                final_counts.update(results)
                print(f"  ✓ {hostname} completed")
    
    reduce_time = time.time() - reduce_start
    print(f"  Reduce phase took: {reduce_time:.2f} seconds")
    
    # 5. Sort and return
    sorted_counts = sorted(final_counts.items(), key=itemgetter(1), reverse=True)
    return sorted_counts

if __name__ == "__main__":
    print("="*60)
    print("OPTIMIZED MAPREDUCE WORD COUNT")
    print("="*60)
    
    # Download dataset
    input_files = download()
    
    # Wait for workers
    print("\nWaiting for workers to start...")
    time.sleep(5)
    
    # Run MapReduce
    print("\n" + "="*60)
    print("STARTING MAPREDUCE")
    print("="*60)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(input_files)
    end_time = time.time()
    
    # Print results
    print('\n' + '='*60)
    print('TOP 20 WORDS BY FREQUENCY')
    print('='*60)
    top20 = word_counts[:20]
    longest = max(len(word) for word, count in top20) if top20 else 0
    
    for i, (word, count) in enumerate(top20, 1):
        print(f'{i}.\t{word:<{longest+2}} {count:>8}')
    
    elapsed_time = end_time - start_time
    print('='*60)
    print(f"✓ Total Elapsed Time: {elapsed_time:.2f} seconds")
    print('='*60)