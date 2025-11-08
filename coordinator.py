import rpyc
import collections
import time
import os
import requests
import zipfile
import concurrent.futures


# ---------------- STATIC WORKER CONFIGURATION ----------------
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


class MapReduceCoordinator:
    """Coordinator for managing MapReduce workers via RPyC."""

    def __init__(self, workers):
        self.workers = workers
        self.connections = []

    # ---------------- CONNECTION MANAGEMENT ----------------
    def connect_workers(self):
        print("[Coordinator] Attempting to establish connections to workers...")
        for host, port in self.workers:
            try:
                conn = rpyc.connect(
                    host,
                    port,
                    config={
                        "allow_pickle": True,
                        "allow_all_attrs": True,
                        "allow_getattr": True,
                        "sync_request_timeout": 300,
                    },
                )
                self.connections.append(conn)
                print(f"[Coordinator] Connected successfully to {host}:{port}")
            except Exception as err:
                print(f"[Coordinator] WARNING: could not connect to {host}:{port} — {err}")

        if not self.connections:
            raise RuntimeError("No workers available — exiting.")

    # ---------------- MAP PHASE ----------------
    def perform_map_phase(self, chunks):
        print(f"[Coordinator] Initiating map phase with {len(chunks)} text chunks...")

        def _map_task(params):
            idx, chunk = params
            for attempt in range(len(self.connections)):
                try:
                    worker = self.connections[(idx + attempt) % len(self.connections)]
                    print(f"[MAP] Assigning chunk {idx} → worker-{(idx % len(self.connections)) + 1} (try {attempt + 1})")
                    result = worker.root.map(chunk)
                    result = rpyc.utils.classic.obtain(result)
                    return result
                except Exception as ex:
                    print(f"[MAP] Error on chunk {idx}: {ex}")
                    time.sleep(1)
            raise RuntimeError(f"Chunk {idx} failed on all workers.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.connections)) as executor:
            mapped = list(executor.map(_map_task, [(i, chunk) for i, chunk in enumerate(chunks)]))

        print(f"[Coordinator] MAP phase completed — {len(mapped)} partial maps received.")
        return mapped

    # ---------------- SHUFFLE ----------------
    def shuffle_results(self, mapped_data):
        print("[Coordinator] Shuffling intermediate results (group by word key)...")
        grouped = collections.defaultdict(list)
        for partial in mapped_data:
            for k, v in partial.items():
                grouped[k].append(v)
        print(f"[Coordinator] Shuffle complete — {len(grouped)} unique keys grouped.")
        return grouped

    # ---------------- REDUCE PHASE ----------------
    def perform_reduce_phase(self, grouped_data):
        print("[Coordinator] Starting reduce phase...")
        partitions = self.partition_dict(grouped_data, len(self.connections))

        def _reduce_task(params):
            idx, partition = params
            for attempt in range(len(self.connections)):
                try:
                    worker = self.connections[(idx + attempt) % len(self.connections)]
                    print(f"[REDUCE] Assigning partition {idx} → worker-{(idx % len(self.connections)) + 1} (try {attempt + 1})")
                    result = worker.root.reduce(partition)
                    return rpyc.utils.classic.obtain(result)
                except Exception as ex:
                    print(f"[REDUCE] Error on partition {idx}: {ex}")
                    time.sleep(1)
            raise RuntimeError(f"Partition {idx} failed after retries.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.connections)) as executor:
            reduced = list(executor.map(_reduce_task, [(i, p) for i, p in enumerate(partitions)]))

        print(f"[Coordinator] REDUCE phase finished — {len(reduced)} partitions processed.")
        return reduced

    # ---------------- UTILS ----------------
    @staticmethod
    def partition_dict(d, n):
        buckets = [collections.defaultdict(list) for _ in range(n)]
        for k, v in d.items():
            buckets[hash(k) % n][k].extend(v)
        return buckets

    @staticmethod
    def split_text(text, n):
        lines = text.splitlines()
        base, extra = divmod(len(lines), n)
        chunks, start = [], 0
        for i in range(n):
            size = base + (1 if i < extra else 0)
            end = start + size
            chunks.append("\n".join(lines[start:end]))
            start = end
        print(f"[Coordinator] Input split into {len(chunks)} chunks.")
        return chunks

    # ---------------- JOB EXECUTION ----------------
    def run_job(self, text):
        job_start = time.time()
        self.connect_workers()
        chunks = self.split_text(text, len(self.workers) * 8)

        mapped = self.perform_map_phase(chunks)
        grouped = self.shuffle_results(mapped)
        reduced = self.perform_reduce_phase(grouped)

        final_output = {}
        for segment in reduced:
            final_output.update(segment)

        sorted_counts = sorted(final_output.items(), key=lambda x: x[1], reverse=True)
        total_time = time.time() - job_start
        print(f"[Coordinator] Completed full MapReduce in {total_time:.2f}s")
        return sorted_counts


# ---------------- HELPER FUNCTIONS ----------------
def download_dataset(url):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(base_dir, "txt")
    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(url)
    archive_path = os.path.join(target_dir, filename)
    extracted_path = archive_path.replace(".zip", "")

    if os.path.exists(extracted_path):
        print(f"[Dataset] Using existing extracted data at {extracted_path}")
        return extracted_path

    if not os.path.exists(archive_path):
        print(f"[Dataset] Downloading dataset from {url} ...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(archive_path, "wb") as f:
            for block in response.iter_content(chunk_size=8192):
                f.write(block)
        print(f"[Dataset] Download completed → {archive_path}")

    with zipfile.ZipFile(archive_path, "r") as z:
        z.extractall(target_dir)
    print(f"[Dataset] Extracted → {extracted_path}")

    return extracted_path


def load_text(file_path):
    print(f"[Dataset] Loading dataset text from {file_path}...")
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()
    print(f"[Dataset] Loaded {len(data):,} characters.")
    return data


# ---------------- MAIN EXECUTION ----------------
if __name__ == "__main__":
    dataset_url = os.getenv("DATA_URL", "https://mattmahoney.net/dc/enwik9.zip")
    dataset_path = download_dataset(dataset_url)
    text_data = load_text(dataset_path)

    master = MapReduceCoordinator(WORKERS)
    results = master.run_job(text_data)

    if results:
        print("\nTOP 20 WORDS\n------------------")
        top20 = results[:20]
        longest = max(len(word) for word, _ in top20)
        for idx, (word, count) in enumerate(top20, start=1):
            print(f"{idx:02d}. {word:<{longest}} : {count}")

    print("\n[Program] Execution complete.")
