import rpyc
import time
from collections import defaultdict
from operator import itemgetter

WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
]

def split_text(text, n_workers):
    """Split text into n_workers chunks"""
    chunk_size = len(text) // n_workers
    chunks = []
    
    for i in range(n_workers):
        start = i * chunk_size
        end = start + chunk_size if i < n_workers - 1 else len(text)
        chunks.append(text[start:end])
    
    return chunks

def mapreduce_wordcount(text):
    n_workers = len(WORKERS)
    
    # 1. Split text into chunks
    print(f"Splitting data into {n_workers} chunks...")
    chunks = split_text(text, n_workers)
    print(f"  Chunk 1 length: {len(chunks[0])} chars")
    print(f"  Chunk 2 length: {len(chunks[1])} chars")
    
    # 2. MAP PHASE
    print("\nMAP PHASE: Sending chunks to workers...")
    map_results = []
    
    for i, (hostname, port) in enumerate(WORKERS):
        try:
            print(f"  Connecting to {hostname}...")
            conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
            print(f"  Sending chunk {i+1} to {hostname}...")
            result = conn.root.exposed_map(chunks[i])
            print(f"  ✓ {hostname} returned {len(result)} word pairs")
            map_results.extend(result)
            conn.close()
        except Exception as e:
            print(f"  ✗ Failed on {hostname}: {e}")
            return None
    
    print(f"\nTotal intermediate pairs: {len(map_results)}")
    
    # 3. SHUFFLE PHASE
    print("\nSHUFFLE PHASE: Grouping intermediate results...")
    grouped = defaultdict(list)
    for word, count in map_results:
        grouped[word].append((word, count))
    
    print(f"  Unique words: {len(grouped)}")
    
    # 4. REDUCE PHASE
    print("\nREDUCE PHASE: Aggregating counts...")
    words = list(grouped.keys())
    words_per_worker = len(words) // n_workers
    
    final_counts = {}
    
    for i, (hostname, port) in enumerate(WORKERS):
        start_idx = i * words_per_worker
        end_idx = start_idx + words_per_worker if i < n_workers - 1 else len(words)
        worker_words = words[start_idx:end_idx]
        
        try:
            print(f"  Connecting to {hostname}...")
            conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
            
            for word in worker_words:
                total = conn.root.exposed_reduce(grouped[word])
                final_counts[word] = total
            
            conn.close()
            print(f"  ✓ {hostname} processed {len(worker_words)} unique words")
        except Exception as e:
            print(f"  ✗ Failed on {hostname}: {e}")
            return None
    
    # 5. Sort results
    sorted_counts = sorted(final_counts.items(), key=itemgetter(1), reverse=True)
    return sorted_counts

if __name__ == "__main__":
    print("="*60)
    print("MAPREDUCE TEST - SMALL DATASET")
    print("="*60)
    
    # Read test file
    print("\nReading test.txt...")
    try:
        with open('test.txt', 'r', encoding='utf-8') as f:
            text = f.read()
        print(f"  File size: {len(text)} characters")
        print(f"  Content preview: {text[:100]}...")
    except FileNotFoundError:
        print("ERROR: test.txt not found!")
        print("Create test.txt in the same directory")
        exit(1)
    
    # Wait for workers
    print("\nWaiting for workers to start...")
    time.sleep(5)
    
    # Run MapReduce
    print("\n" + "="*60)
    print("STARTING MAPREDUCE")
    print("="*60)
    
    start_time = time.time()
    word_counts = mapreduce_wordcount(text)
    end_time = time.time()
    
    if word_counts:
        # Print ALL results (it's small!)
        print("\n" + "="*60)
        print("WORD COUNT RESULTS")
        print("="*60)
        
        for i, (word, count) in enumerate(word_counts, 1):
            print(f'{i}.\t{word:<15} {count:>5}')
        
        elapsed_time = end_time - start_time
        print("\n" + "="*60)
        print(f"✓ TEST PASSED!")
        print(f"✓ Elapsed Time: {elapsed_time:.2f} seconds")
        print("="*60)
    else:
        print("\n✗ TEST FAILED - Check errors above")