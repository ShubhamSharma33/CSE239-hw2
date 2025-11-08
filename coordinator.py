import rpyc
import collections
import time
import os
import requests
import zipfile
import sys
import concurrent.futures
import json
import pickle

# worker hostnames and ports for rpyc connections
WORKERS = [
    ("worker-1", 18865),
    ("worker-2", 18865),
    ("worker-3", 18865),
    ("worker-4", 18865),
    ("worker-5", 18865),
    ("worker-6", 18865),
    ("worker-7", 18865),
    ("worker-8", 18865)
]


def mapreduce_wordcount(text):
    start_time = time.time()
    """
    Executes a complete MapReduce word count job across all workers.
    1. Split text into chunks
    2. Connect to workers
    3. MAP PHASE: Send chunks in PARALLEL with retry logic
    4. SHUFFLE PHASE: Group intermediate pairs by key
    5. REDUCE PHASE: Send partitions in PARALLEL with retry logic
    6. FINAL AGGREGATION
    """
    print("[Coordinator] Starting MapReduce job...")


    worker_connections = []
    for host, port in WORKERS:
        try:
            print(f"[COORDINATOR] Establishing connection with {host} on port {port}...")
            conn = rpyc.connect(host, port, config={
                "allow_pickle": True,    
                "allow_all_attrs": True,    
                "allow_getattr": True,
                "sync_request_timeout": 250,
            })
            worker_connections.append(conn)
        except Exception as e:
            print(f"[Coordinator] Failed to connect to {host}:{port} - {e}")

    if not worker_connections:
        raise RuntimeError("[COORDINATOR] Fatal: No worker nodes available for processing!")
    

    n_workers = len(worker_connections)

    # create more chunks than workers for better load balancing
    num_chunks = n_workers*8
    chunks = split_text(text, num_chunks)

    # using thread pool to send chunks to workers concurrently
    thread_pool_size = min(len(chunks), len(worker_connections)*8)
    print("[Coordinator] Starting MAP phase with parallel execution...")
    
    def process_chunk(args):
        """Helper function to process a chunk with retry logic"""
        chunk_idx, chunk, worker_connections = args
        max_retries = len(worker_connections)
        
        for retry_count in range(max_retries):
            try:
                worker_idx = (chunk_idx + retry_count) % len(worker_connections)
                conn = worker_connections[worker_idx]
                
                print(f"[COORDINATOR] Assigning segment {chunk_idx} to node-{worker_idx+1} (try #{retry_count+1})")
                
                result = conn.root.map(chunk)
                
                print(f"[COORDINATOR] Node-{worker_idx+1} processed segment {chunk_idx}: found {len(result)} distinct words")
                return result
                
            except Exception as e:
                print(f"[Coordinator] Error on worker-{worker_idx+1} for chunk {chunk_idx}: {e}")
                if retry_count >= max_retries - 1:
                    raise RuntimeError(f"Failed to process chunk {chunk_idx} after {max_retries} attempts")
                print(f"[Coordinator] Retrying chunk {chunk_idx}...")
                time.sleep(1)


    with concurrent.futures.ThreadPoolExecutor(thread_pool_size) as executor:
        chunk_args = [(i, chunk, worker_connections) for i, chunk in enumerate(chunks)]
        map_results = list(executor.map(process_chunk, chunk_args))
    
    print(f"[COORDINATOR] Mapping finished. Collected {len(map_results)} intermediate results.")
    

    print("[COORDINATOR] Reorganizing intermediate data by keys...")
    
    # grouping all values by key across all map results
    grouped = collections.defaultdict(list)
    for partial_dict in map_results:
        for key, value in partial_dict.items():
            grouped[key].append(value)


    print("[COORDINATOR] Beginning parallel reduction across worker nodes...")
    partitions = partition_dict(grouped, n_workers)
    
    def process_partition(args):
        """Helper function to process a partition with retry logic"""
        partition_idx, partition, worker_connections = args
        max_retries = len(worker_connections)
        
         # retrying with different workers if one fails
        for retry_count in range(max_retries):
            try:
                worker_idx = (partition_idx + retry_count) % len(worker_connections)
                conn = worker_connections[worker_idx]
                
                print(f"[Coordinator] Partition {partition_idx} -> worker-{worker_idx+1} (attempt {retry_count+1})")
                
                # partition_str = json.dumps(partition)
                # result = conn.root.reduce(partition_str)
                result=conn.root.reduce(partition)
                
                print(f"[Coordinator] Worker-{worker_idx+1} completed partition {partition_idx}: {len(result)} keys")
                return result
                
            except Exception as e:
                print(f"[Coordinator] Error on worker-{worker_idx+1} for partition {partition_idx}: {e}")
                if retry_count >= max_retries - 1:
                    raise RuntimeError(f"Failed to process partition {partition_idx} after {max_retries} attempts")
                print(f"[Coordinator] Retrying partition {partition_idx}...")
                time.sleep(1)
    

    with concurrent.futures.ThreadPoolExecutor(thread_pool_size) as executor:
        partition_args = [(i, partition, worker_connections) for i, partition in enumerate(partitions)]
        reduced_results = list(executor.map(process_partition, partition_args))
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} before Aggregation to count Bottleneck".format(elapsed_time))


    print("[COORDINATOR] giving final results...")

    final_counts = {}
    for partial in reduced_results:
        final_counts.update(partial)


    word_counts = sorted(final_counts.items(), key=lambda x: x[1], reverse=True)

    print("[Coordinator] MapReduce completed!")

    return word_counts


def split_text(text, n):
    """
    Split text into n chunks aligned to full lines, never cutting inside words.
    """
    lines = text.splitlines()
    total = len(lines)
    
    # Calculate chunk sizes (distribute remainder evenly)
    base_size = total // n
    remainder = total % n
    
    chunks = []
    start = 0
    
    for i in range(n):
        chunk_size = base_size + (1 if i < remainder else 0)
        end = start + chunk_size
        
        chunk = "\n".join(lines[start:end])
        chunks.append(chunk)
        start = end
    
    print(f"[COORDINATOR] Text partitioned into {len(chunks)} segments from {total} lines")
    return chunks


def partition_dict(d, n):
    """
    Partition dictionary keys into n buckets using hash function.
    """
    partitions = [collections.defaultdict(list) for _ in range(n)]
    
    for key, value in d.items():
        index = hash(key) % n  # using hash-based partitioning to distribute keys evenly
        partitions[index][key].extend(value)
    
    return partitions


def download(url):
    """Downloads and unzips a wikipedia dataset in txt/."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "txt")
    os.makedirs(output_dir, exist_ok=True)
    
    filename = url.split('/')[-1]
    filepath = os.path.join(output_dir, filename)
    extracted_path = filepath.replace('.zip', '')
    
    # Skip if already extracted
    if os.path.exists(extracted_path):
        print(f"[Coordinator] Dataset already exists at {extracted_path}")
        return extracted_path
    
    # Download if missing
    if not os.path.exists(filepath):
        print(f"[Coordinator] Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[Coordinator] Downloaded to {filepath}")
    
    # Extract if it's a zip file
    if filepath.endswith('.zip'):
        print(f"[Coordinator] Extracting {filepath}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"[Coordinator] Extracted to {extracted_path}")
        return extracted_path

    return filepath


def read_dataset(path):
    """
    Reads the dataset file returned by download() and returns its text content.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"[Coordinator] Dataset not found at {path}")

    print(f"[Coordinator] Reading dataset from {path}...")
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        data = f.read()

    print(f"[Coordinator] Loaded file: {path}, size: {len(data):,} characters")
    return data


if __name__ == "__main__":
    # DOWNLOAD AND UNZIP DATASET
    textPath = os.getenv("DATA_URL", "https://mattmahoney.net/dc/enwik9.zip")
    textPath = download(textPath)
    text = read_dataset(textPath)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(text)
    
    if word_counts:
        print('\nTOP 20 WORDS BY FREQUENCY\n')
        top20 = word_counts[0:20]
        longest = max(len(word) for word, count in top20)
        
        i = 1
        for word, count in top20:
            print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
            i = i + 1
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total execution time: {} seconds".format(elapsed_time))