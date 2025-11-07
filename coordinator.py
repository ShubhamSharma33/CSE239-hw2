import rpyc
import time
import urllib.request
import zipfile
import os
import glob
from collections import defaultdict
from operator import itemgetter

WORKERS = [
    ("worker-1", 18861)
]

def download(url='https://mattmahoney.net/dc/enwik8.zip'):
    """Download and extract dataset"""
    filename = url.split('/')[-1]
    
    # Skip if already exists
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

def mapreduce_wordcount(input_files):
    n_workers = len(WORKERS)
    
    # 1. Split text into chunks
    print(f"Splitting data into {n_workers} chunks...")
    chunks = split_text(input_files, n_workers)
    
    # 2. MAP PHASE: Connect to workers and send chunks
    print("MAP PHASE: Sending chunks to workers...")
    map_results = []
    
    for i, (hostname, port) in enumerate(WORKERS):
        try:
            conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
            result = conn.root.exposed_map(chunks[i])
            map_results.extend(result)
            conn.close()
            print(f"  ✓ Completed map on {hostname}")
        except Exception as e:
            print(f"  ✗ Failed on {hostname}: {e}")
    
    # 3. SHUFFLE PHASE: Group by key
    print("SHUFFLE PHASE: Grouping intermediate results...")
    grouped = defaultdict(list)
    for word, count in map_results:
        grouped[word].append((word, count))
    
    # 4. REDUCE PHASE: Send grouped data to workers
    print("REDUCE PHASE: Aggregating counts...")
    words = list(grouped.keys())
    words_per_worker = len(words) // n_workers
    
    final_counts = {}
    
    for i, (hostname, port) in enumerate(WORKERS):
        start_idx = i * words_per_worker
        end_idx = start_idx + words_per_worker if i < n_workers - 1 else len(words)
        worker_words = words[start_idx:end_idx]
        
        try:
            conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
            
            for word in worker_words:
                total = conn.root.exposed_reduce(grouped[word])
                final_counts[word] = total
            
            conn.close()
            print(f"  ✓ Completed reduce on {hostname}")
        except Exception as e:
            print(f"  ✗ Failed on {hostname}: {e}")
    
    # 5. Sort and return top results
    sorted_counts = sorted(final_counts.items(), key=itemgetter(1), reverse=True)
    return sorted_counts

if __name__ == "__main__":
    print("Starting MapReduce Word Count...")
    
    # Download dataset
    input_files = download()
    
    # Wait for workers to be ready
    print("Waiting for workers to start...")
    time.sleep(5)
    
    # Run MapReduce
    start_time = time.time()
    word_counts = mapreduce_wordcount(input_files)
    end_time = time.time()
    
    # Print results
    print('\n' + '='*50)
    print('TOP 20 WORDS BY FREQUENCY')
    print('='*50)
    top20 = word_counts[:20]
    longest = max(len(word) for word, count in top20) if top20 else 0
    
    for i, (word, count) in enumerate(top20, 1):
        print(f'{i}.\t{word:<{longest+2}} {count:>8}')
    
    elapsed_time = end_time - start_time
    print('='*50)
    print(f"Elapsed Time: {elapsed_time:.2f} seconds")
    print('='*50)