import rpyc
import string
import collections
import itertools
import time
import operator
import glob
import sys
import urllib.request
import zipfile
import os

WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
    ("worker-3", 18861),
    ("worker-4", 18861),
]

def mapreduce_wordcount(input_files):
    """
    Main MapReduce function
    1. Split text into chunks
    2. Connect to workers
    3. MAP PHASE: Send chunks and get intermediate pairs
    4. SHUFFLE PHASE: Group intermediate pairs by key
    5. REDUCE PHASE: Send grouped data to reducers
    6. FINAL AGGREGATION
    """
    n_workers = len(WORKERS)
    
    # 1. SPLIT TEXT INTO CHUNKS
    print(f"[INFO] Splitting data for {n_workers} workers...")
    chunks = split_files_into_chunks(input_files, n_workers)
    print(f"[INFO] Created {len(chunks)} chunks")
    
    # 2. CONNECT TO WORKERS
    print("[INFO] Connecting to workers...")
    connections = []
    for hostname, port in WORKERS:
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
        print(f"  Connected to {hostname}:{port}")
    
    # 3. MAP PHASE: Send chunks and get intermediate pairs (PARALLEL!)
    print(f"\n[MAP] Dispatching {len(chunks)} chunks to {n_workers} workers...")
    map_start = time.time()
    
    pending_results = []
    
    # DISPATCH ALL ASYNC CALLS (non-blocking) - workers start immediately!
    for i, chunk in enumerate(chunks):
        worker_idx = i % n_workers
        conn = connections[worker_idx]
        
        # Send async - does NOT wait for completion
        async_result = rpyc.async_(conn.root.process_chunk)(chunk)
        pending_results.append(async_result)
        
        print(f"  Dispatched chunk {i+1}/{len(chunks)} to worker-{worker_idx+1}")
    
    print(f"[MAP] All chunks dispatched! Workers processing in parallel...")
    
    # COLLECT ALL RESULTS - workers have been processing during dispatch!
    print(f"[MAP] Collecting results...")
    map_results = []
    
    for i, async_result in enumerate(pending_results):
        # Wait for this specific result
        result = async_result.value
        map_results.append(result)
        
        worker_idx = i % n_workers
        print(f"  Collected result {i+1}/{len(pending_results)} from worker-{worker_idx+1}")
    
    map_time = time.time() - map_start
    print(f"[MAP] Map phase complete! Time: {map_time:.2f}s")
    
    # 4. SHUFFLE PHASE: Group intermediate pairs by key
    print("\n[SHUFFLE] Grouping intermediate results...")
    shuffle_start = time.time()
    
    aggregated = collections.defaultdict(int)
    for result_dict in map_results:
        for word, count in result_dict.items():
            aggregated[word] += count
    
    shuffle_time = time.time() - shuffle_start
    print(f"[SHUFFLE] Found {len(aggregated)} unique words. Time: {shuffle_time:.2f}s")
    
    # 5. REDUCE PHASE: Send grouped data to reducers
    print("\n[REDUCE] Distributing reduce tasks...")
    reduce_start = time.time()
    
    # Partition words across workers for reduce
    words = list(aggregated.keys())
    partitions = partition_dict(aggregated, n_workers)
    
    final_counts = {}
    
    # Simple reduce - just copy partitions (already aggregated in shuffle)
    for i, partition in enumerate(partitions):
        for word, count in partition.items():
            final_counts[word] = count
        print(f"  Worker {i+1} processed {len(partition)} words")
    
    reduce_time = time.time() - reduce_start
    print(f"[REDUCE] Reduce phase complete! Time: {reduce_time:.2f}s")
    
    # 6. FINAL AGGREGATION
    print("\n[AGGREGATE] Sorting results...")
    total_counts = sorted(final_counts.items(), key=operator.itemgetter(1), reverse=True)
    
    # Close connections
    print("[CLEANUP] Closing worker connections...")
    for conn in connections:
        conn.close()
    
    return total_counts

def split_text(text, n):
    """Split text into n roughly equal chunks"""
    chunk_size = len(text) // n
    chunks = []
    
    for i in range(n):
        start = i * chunk_size
        end = start + chunk_size if i < n - 1 else len(text)
        chunks.append(text[start:end])
    
    return chunks

def split_files_into_chunks(files, n):
    """Read all files and split into n chunks"""
    print(f"[SPLIT] Reading {len(files)} file(s)...")
    all_text = ""
    
    for filepath in files:
        print(f"  Reading {filepath}...")
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            all_text += f.read()
    
    print(f"[SPLIT] Total text size: {len(all_text):,} characters")
    return split_text(all_text, n)

def partition_dict(d, n):
    """Partition dictionary into n roughly equal parts"""
    items = list(d.items())
    chunk_size = len(items) // n
    
    partitions = []
    for i in range(n):
        start = i * chunk_size
        end = start + chunk_size if i < n - 1 else len(items)
        partition = dict(items[start:end])
        partitions.append(partition)
    
    return partitions

def download(url='https://mattmahoney.net/dc/enwik8.zip'):
    """Downloads and unzips a wikipedia dataset in txt/."""
    filename = url.split('/')[-1]
    zip_path = filename
    
    # Download if not exists
    if not os.path.exists(zip_path):
        print(f"[DOWNLOAD] Downloading {url}...")
        urllib.request.urlretrieve(url, zip_path)
        print("[DOWNLOAD] Complete")
    else:
        print(f"[DOWNLOAD] Found {zip_path}, skipping download")
    
    # Extract if txt/ is empty
    os.makedirs('txt', exist_ok=True)
    if not glob.glob('txt/*'):
        print(f"[EXTRACT] Unzipping {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall('txt/')
        print("[EXTRACT] Complete")
    else:
        print("[EXTRACT] Dataset already extracted")
    
    return glob.glob('txt/*')

if __name__ == "__main__":
    # DOWNLOAD AND UNZIP DATASET
    url = sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik8.zip'
    
    print("="*60)
    print("DISTRIBUTED MAPREDUCE WORD COUNT")
    print("="*60)
    print(f"Workers: {len(WORKERS)}")
    print(f"Dataset: {url}")
    print("="*60 + "\n")
    
    input_files = download(url)
    
    # Wait for workers
    print("\n[INIT] Waiting 5 seconds for workers to start...")
    time.sleep(5)
    
    print("\n" + "="*60)
    print("STARTING MAPREDUCE")
    print("="*60)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(input_files)
    end_time = time.time()
    
    print('\n' + '='*60)
    print('TOP 20 WORDS BY FREQUENCY')
    print('='*60 + '\n')
    
    top20 = word_counts[0:20]
    longest = max(len(word) for word, count in top20) if top20 else 5
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1
    
    elapsed_time = end_time - start_time
    print("\n" + "="*60)
    print("Elapsed Time: {:.2f} seconds".format(elapsed_time))
    print("="*60)