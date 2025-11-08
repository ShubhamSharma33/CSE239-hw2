import rpyc
import string
import collections
import threading
import os
import json

class MapReduceService(rpyc.Service):

    # common words to exclude from word count (ref: hw-1)
    STOP_WORDS = set([
                'a','an','and','are','as','be','by','for','if','in',
                'is','it','of','or','py','rst','that','the','to','with',
            ])
            
    # translation table to replace all punctuation with spaces
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    def exposed_map(self, text_chunk):
        """Map step: tokenize and count words in text chunk."""
        # with self.lock:
            
        counts = collections.defaultdict(int)

        for line in text_chunk.splitlines():
            if line.lstrip().startswith('..'):
                continue
            line = line.translate(self.TR)
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in self.STOP_WORDS:
                    counts[word] += 1

            print(f"[Worker] Mapped chunk: {len(counts)} unique words")
            return dict(counts)
    
    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""
        # with self.lock:
            # Using Counter for efficient aggregation
        # grouped_items = json.loads(grouped_items_str)
        reduced = collections.Counter()
        for word, values in grouped_items.items():
            reduced[word] = sum(values)
            
        print(f"[Worker] Reduced {len(reduced)} keys")
        return dict(reduced)


if __name__ == "__main__":
    import sys
    from rpyc.utils.server import ThreadedServer
    
    port = int(os.getenv("WORKER_PORT", sys.argv[1] if len(sys.argv) > 1 else 18865))
    # starting rpyc server with extended timeout for large datasets
    t = ThreadedServer(
        MapReduceService,
        port=18865,
        protocol_config={
            "allow_pickle": True,
            "allow_all_attrs": True,
            "allow_getattr": True,
            "sync_request_timeout": 300,
        },
    )
    print(f"[Worker] RPyC Worker running on port {port}...")
    t.start()