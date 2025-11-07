import rpyc
import time
import urllib.request
import zipfile
import os
import glob
from collections import defaultdict
from operator import itemgetter
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

def get_file_chunks(files, n_workers):
    """Yield file chunks without loading entire file"""
    # Calculate total size
    total_size = sum(os.path.getsize(f) for f in files)
    chunk_size = total_size // n_workers
    
    # Read and yield chunks
    current_chunk = []
    current_size = 0
    chunks_yielded = 0
    
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            BLOCK_SIZE = 1024 * 1024  # 1MB blocks
            
            while True:
                block = f.read(BLOCK_SIZE)
                if not block:
                    break
                
                current_chunk.append(block)
                current_size += len(block)
                
                # Yield chunk when it's full
                if current_size >= chunk_size and chunks_yielded < n_workers - 1:
                    yield ''.join(current_chunk)
                    current_chunk = []
                    current_size = 0
                    chunks_yielded += 1
    
    # Yield remaining data
    if current_chunk:
        yield ''.join(current_chunk)

def map_worker(worker_info, chunk):
    """Execute map on a single worker"""
    hostname, port = worker_info
    try:
        conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
        result = conn.root.exposed_map(chunk)
        result = list(result)  # Convert to local list
        conn.close()
        return (hostname, result, None)
    except Exception as e:
        return (hostname, None, str(e))

def reduce_worker(worker_info, word_groups):
    """Execute reduce on a single worker"""
    hostname, port = worker_info
    results = {}
    try:
        conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
        
        for word, pairs in word_groups.items():
            total = conn.root.exposed_reduce(pairs)
            results[word] = int(total)
        
        conn.close()
        return (hostname, results, None)
    except Exception as e:
        return (hostname, None, str(e))

def mapreduce_wordcount(input_files):
    n_workers = len(WORKERS)
    
    # 1. Get chunks as generator (doesn't load everything!)
    print(f"Preparing to split data into {n_workers} chunks...")
    chunks = list(get_file_chunks(input_files, n_workers))
    print(f"  Split complete. Chunk sizes: {[f'{len(c)/1024/1024:.1f}MB' for c in chunks]}")
    
    # 2. MAP PHASE - PARALLEL
    print("\nMAP PHASE: Sending chunks to workers IN PARALLEL...")
    map_start = time.time()
    
    map_results = []
    
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(map_worker, WORKERS[i], chunks[i]): i 
            for i in range(len(chunks))
        }
        
        for future in as_completed(futures):
            hostname, result, error = future.result()
            if error:
                print(f"  ✗ Failed: {error}")
            else:
                map_results.extend(result)
                print(f"  ✓ Worker completed ({len(result)} pairs)")
    
    map_time = time.time() - map_start
    print(f"  Map phase: {map_time:.2f}s")
    
    # 3. SHUFFLE PHASE
    print("\nSHUFFLE PHASE: Grouping results...")
    shuffle_start = time.time()
    
    grouped = defaultdict(list)
    for word, count in map_results:
        grouped[word].append((word, count))
    
    shuffle_time = time.time() - shuffle_start
    print(f"  Shuffle phase: {shuffle_time:.2f}s ({len(grouped)} unique words)")
    
    # 4. REDUCE PHASE - PARALLEL
    print("\nREDUCE PHASE: Aggregating counts IN PARALLEL...")
    reduce_start = time.time()
    
    words = list(grouped.keys())
    words_per_worker = len(words) // n_workers
    
    worker_batches = []
    for i in range(n_workers):
        start_idx = i * words_per_worker
        end_idx = start_idx + words_per_worker if i < n_workers - 1 else len(words)
        worker_words = words[start_idx:end_idx]
        batch = {word: grouped[word] for word in worker_words}
        worker_batches.append(batch)
    
    final_counts = {}
    
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(reduce_worker, WORKERS[i], worker_batches[i]): i 
            for i in range(n_workers)
        }
        
        for future in as_completed(futures):
            hostname, results, error = future.result()
            if error:
                print(f"  ✗ Failed: {error}")
            else:
                final_counts.update(results)
                print(f"  ✓ Worker completed ({len(results)} words)")
    
    reduce_time = time.time() - reduce_start
    print(f"  Reduce phase: {reduce_time:.2f}s")
    
    # Clear chunks from memory
    chunks = None
    
    # 5. Sort and return
    sorted_counts = sorted(final_counts.items(), key=itemgetter(1), reverse=True)
    return sorted_counts

if __name__ == "__main__":
    print("="*60)
    print("MEMORY-OPTIMIZED MAPREDUCE WORD COUNT")
    print("="*60)
    
    input_files = download()
    
    print("\nWaiting for workers to start...")
    time.sleep(5)
    
    print("\n" + "="*60)
    print("STARTING MAPREDUCE")
    print("="*60)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(input_files)
    end_time = time.time()
    
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
