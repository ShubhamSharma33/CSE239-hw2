import rpyc
import string
import collections
import threading


class WorkerNode(rpyc.Service):
    """RPyC Service handling map and reduce operations for word counting."""

    def __init__(self):
        # Lock ensures thread-safe map/reduce execution
        self._mutex = threading.Lock()

    # ---------------- MAP FUNCTION ----------------
    def exposed_map(self, data_chunk):
        """Performs the mapping step: tokenizes text and counts valid words."""
        with self._mutex:
            punctuation_table = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
            ignore_words = {
                'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
                'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with'
            }

            local_count = collections.defaultdict(int)

            for raw_line in data_chunk.splitlines():
                if raw_line.strip().startswith('..'):
                    continue  # ignore special formatted lines

                clean_line = raw_line.translate(punctuation_table)
                for token in clean_line.split():
                    word = token.lower()
                    if word.isalpha() and word not in ignore_words:
                        local_count[word] += 1

            print(f"[WorkerNode] Completed MAP phase: {len(local_count)} words counted")
            return dict(local_count)

    # ---------------- REDUCE FUNCTION ----------------
    def exposed_reduce(self, key_groups):
        """Performs the reduce step: sums word counts for each key group."""
        from rpyc.utils.classic import obtain

        with self._mutex:
            key_groups = obtain(key_groups)  # unmarshal RPyC objects
            combined = collections.Counter()

            for term, occurrences in key_groups.items():
                combined[term] = sum(occurrences)

            print(f"[WorkerNode] Reduced batch with {len(combined)} unique keys")
            return dict(combined)


# ---------------- SERVER ENTRY POINT ----------------
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    PORT = 18865
    print(f"[WorkerNode] Starting service listener on port {PORT} ...")

    service = ThreadedServer(
        WorkerNode,
        port=PORT,
        hostname="0.0.0.0",
        protocol_config={
            "allow_pickle": True,
            "allow_all_attrs": True,
            "allow_getattr": True,
            "sync_request_timeout": 300,
        },
    )

    service.start()