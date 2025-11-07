import rpyc
from collections import Counter
import re

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """Map: tokenize text and return word counts"""
        words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
        return [(word, 1) for word in words]
    
    def exposed_reduce(self, word_count_pairs):
        """Reduce: sum up counts for words"""
        total = sum(count for word, count in word_count_pairs)
        return total

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    print("Worker starting on port 18861...")
    t = ThreadedServer(MapReduceService, port=18861)
    t.start()