#!/usr/bin/env python3
"""
Generate docker-compose.yml for different number of workers
Usage: python3 generate_compose.py <num_workers>
"""

import sys

def generate_compose(num_workers):
    template = f"""version: "3.8"

services:
  coordinator:
    build: .
    container_name: coordinator
    hostname: coordinator
    command: python coordinator.py
    networks:
      - mapreduce-net
    environment:
      - NUM_WORKERS={num_workers}
      - PYTHONUNBUFFERED=1
    ports:
      - "18860:18860"
    volumes:
      - ./txt:/app/txt
"""

    deps = [f"      - worker-{i}" for i in range(1, num_workers + 1)]
    if deps:
        template += "    depends_on:\n" + "\n".join(deps) + "\n"

    template += "\n"

    for i in range(1, num_workers + 1):
        template += f"""  worker-{i}:
    build: .
    container_name: worker-{i}
    hostname: worker-{i}
    command: python worker.py
    networks:
      - mapreduce-net
    environment:
      - COORDINATOR_HOST=coordinator
      - PYTHONUNBUFFERED=1
    volumes:
      - ./txt:/app/txt
    depends_on:
      - coordinator

"""

    template += """networks:
  mapreduce-net:
    driver: bridge
"""

    return template


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 generate_compose.py <num_workers>")
        print("Example: python3 generate_compose.py 8")
        sys.exit(1)

    try:
        num_workers = int(sys.argv[1])
        if num_workers < 1:
            print("Error: num_workers must be >= 1")
            sys.exit(1)

        compose_content = generate_compose(num_workers)
        
        with open("docker-compose.yml", "w") as f:
            f.write(compose_content)
        
        print(f"✓ Generated docker-compose.yml with {num_workers} worker(s)")
        print(f"  Run with: docker-compose up --build")

    except ValueError:
        print("Error: num_workers must be an integer")
        sys.exit(1)