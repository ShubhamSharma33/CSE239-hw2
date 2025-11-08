# Quick Testing Guide - Changing Number of Workers

## Method 1: Use Pre-configured Files (Easiest)

I've created separate docker-compose files for each worker count. Use the `-f` flag to specify which one:

### Test with 1 Worker
```bash
docker-compose -f docker-compose.1worker.yml down
docker-compose -f docker-compose.1worker.yml build
docker-compose -f docker-compose.1worker.yml up
```

### Test with 2 Workers
```bash
docker-compose -f docker-compose.2workers.yml down
docker-compose -f docker-compose.2workers.yml build
docker-compose -f docker-compose.2workers.yml up
```

### Test with 4 Workers
```bash
docker-compose -f docker-compose.4workers.yml down
docker-compose -f docker-compose.4workers.yml build
docker-compose -f docker-compose.4workers.yml up
```

### Test with 8 Workers
```bash
docker-compose -f docker-compose.8workers.yml down
docker-compose -f docker-compose.8workers.yml build
docker-compose -f docker-compose.8workers.yml up
```

---

## Method 2: Edit docker-compose.yml Manually

If you prefer to edit the main `docker-compose.yml` file:

### Step 1: Edit Coordinator Environment Variables

Find the `coordinator` service and change:
```yaml
environment:
  - NUM_WORKERS=4    # Change this number (1, 2, 4, or 8)
  - NUM_REDUCE_TASKS=4  # Change this to match NUM_WORKERS
```

### Step 2: Edit depends_on Section

In the same `coordinator` service, update the `depends_on` list:

**For 1 worker:**
```yaml
depends_on:
  - worker-1
```

**For 2 workers:**
```yaml
depends_on:
  - worker-1
  - worker-2
```

**For 4 workers:**
```yaml
depends_on:
  - worker-1
  - worker-2
  - worker-3
  - worker-4
```

**For 8 workers:**
```yaml
depends_on:
  - worker-1
  - worker-2
  - worker-3
  - worker-4
  - worker-5
  - worker-6
  - worker-7
  - worker-8
```

### Step 3: Rebuild and Run

```bash
docker-compose down
docker-compose build
docker-compose up
```

---

## Quick Reference Table

| Workers | NUM_WORKERS | NUM_REDUCE_TASKS | depends_on workers |
|---------|-------------|------------------|-------------------|
| 1       | 1           | 1                | worker-1          |
| 2       | 2           | 2                | worker-1, worker-2 |
| 4       | 4           | 4                | worker-1 through worker-4 |
| 8       | 8           | 8                | worker-1 through worker-8 |

---

## Tips

1. **Always run `docker-compose down`** before switching worker counts to clean up containers
2. **Record execution times** from the output for your assignment
3. **Use smaller dataset (enwik8)** for quick testing, then switch to enwik9 for final benchmarks
4. **Check the output** - it will show "Workers: X" at the start to confirm the configuration

---

## Example Workflow for Testing

```bash
# Test 1 worker
docker-compose -f docker-compose.1worker.yml down
docker-compose -f docker-compose.1worker.yml up
# Record elapsed time from output

# Test 2 workers
docker-compose -f docker-compose.2workers.yml down
docker-compose -f docker-compose.2workers.yml up
# Record elapsed time from output

# Test 4 workers
docker-compose -f docker-compose.4workers.yml down
docker-compose -f docker-compose.4workers.yml up
# Record elapsed time from output

# Test 8 workers
docker-compose -f docker-compose.8workers.yml down
docker-compose -f docker-compose.8workers.yml up
# Record elapsed time from output
```

