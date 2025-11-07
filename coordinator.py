import rpyc
import time

WORKERS = [
    ("worker-1", 18861),
    ("worker-2", 18861),
]

def test_connection():
    print("Waiting for workers to start...")
    time.sleep(5)
    
    for hostname, port in WORKERS:
        try:
            print(f"Connecting to {hostname}:{port}...")
            conn = rpyc.connect(hostname, port, config={"allow_public_attrs": True})
            result = conn.root.exposed_map("hello world test")
            print(f"✓ Success! {hostname} returned: {result[:5]}...")
            conn.close()
        except Exception as e:
            print(f"✗ Failed to connect to {hostname}: {e}")

if __name__ == "__main__":
    test_connection()