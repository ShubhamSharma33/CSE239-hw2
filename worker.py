import rpyc
from collections import Counter
import re

class MapService(rpyc.Service):
    def exposed_process_chunk(self, text):
        """
        Process a text chunk and return word counts
        CRITICAL: Method name must match coordinator's call!
        """
        # Extract words
        words = re.findall(r'\b[a-z]+\b', text.lower())
        
        # Count frequencies
        counts = Counter(words)
        
        # Return as plain dict (important for RPyC)
        return dict(counts)

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    
    print("Worker starting on port 18861...")
    server = ThreadedServer(
        MapService,
        port=18861,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle": True,
            "sync_request_timeout": 120
        }
    )
    server.start()