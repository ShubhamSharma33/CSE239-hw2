import rpyc
from collections import Counter
import re

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """
        Map function: Tokenize text and return word frequency dict
        Returns dict instead of list of tuples for efficient aggregation
        """
        # Extract words (lowercase, alphabetic only)
        words = re.findall(r'\b[a-z]+\b', text_chunk.lower())
        
        # Count words in this chunk
        word_counts = Counter(words)
        
        # Return as regular dict (not Counter, to avoid serialization issues)
        return dict(word_counts)

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    print("Worker starting on port 18861...")
    server = ThreadedServer(MapReduceService, port=18861)
    server.start()