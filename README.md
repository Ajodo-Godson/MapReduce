# MapReduce Distributed System
This is an implementation of a mini version of the google's MapReduce Paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf 



# Milestone 1

A simplified MapReduce implementation demonstrating distributed processing with fault tolerance and state visualization.


1. **Distributed Processing**: Multiple worker nodes process data in parallel
2. **State Visualization**: Real-time system status display every 10 seconds
3. **Fault Tolerance**: Automatic detection and recovery from worker failures

## Project Structure

```
mapreduce-project/
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
│
├── protos/
│   └── mapreduce.proto          # gRPC service definitions
│
├── src/
│   ├── master/
│   │   ├── __init__.py
│   │   ├── server.py            # Master gRPC server
│   │   ├── state.py             # State management
│   │   ├── scheduler.py         # Task scheduling
│   │   └── monitor.py           # Worker health monitoring
│   │
│   └── worker/
│       ├── __init__.py
│       ├── client.py            # Worker implementation
│       └── executor.py          # Task execution logic
│
└── scripts/
    ├── generate_proto.sh        # Compile protobuf files
    └── run_cluster.sh           # Cluster management
```

## Quick Start

### Prerequisites
- Docker
- Docker Compose

### 1. Build and Start the Cluster

```bash
chmod +x scripts/*.sh
./scripts/run_cluster.sh start
```

This starts:
- 1 Master node (coordinator)
- 3 Worker nodes (executors)

### 2. View Real-Time Status

Watch the master node logs to see system status updates:

```bash
docker-compose logs -f master
```

You'll see output like:

```
============================================================
SYSTEM STATUS - 2025-10-24T10:30:15
============================================================
Workers: 3 registered
  worker1: busy (last seen: 10:30:12)
  worker2: idle (last seen: 10:30:11)
  worker3: busy (last seen: 10:30:13)

Tasks:
  Pending: 0
  Running: 2
  Completed: 1
  Failed: 0
============================================================
```

### 3. Test Fault Tolerance

Kill a worker to see automatic recovery:

```bash
./scripts/run_cluster.sh kill-worker worker2
```

Watch the master logs - you'll see:
1. Worker marked as FAILED after missing heartbeats
2. Tasks automatically reassigned to healthy workers
3. System continues processing

### 4. View Individual Worker Logs

```bash
docker-compose logs -f worker1
```

## Testing Scenarios

### Scenario 1: Normal Operation
```bash
./scripts/run_cluster.sh start
docker-compose logs -f master
# Watch all 3 tasks complete successfully
```

### Scenario 2: Worker Failure During Execution
```bash
./scripts/run_cluster.sh start
sleep 10  # Let tasks start
./scripts/run_cluster.sh kill-worker worker2
docker-compose logs -f master
# Watch tasks get reassigned and completed
```

### Scenario 3: Multiple Worker Failures
```bash
./scripts/run_cluster.sh start
sleep 10
./scripts/run_cluster.sh kill-worker worker1
sleep 5
./scripts/run_cluster.sh kill-worker worker3
docker-compose logs -f master
# Watch remaining worker complete all tasks
```

## How It Works

### 1. Architecture

```
┌─────────────────┐
│  Master Node    │
│  - Schedules    │
│  - Monitors     │
│  - Coordinates  │
└────────┬────────┘
         │
    ┌────┴────┬────────┬────────┐
    │         │        │        │
┌───▼───┐ ┌──▼───┐ ┌──▼───┐ ┌──▼───┐
│Worker1│ │Worker2│ │Worker3│ │WorkerN│
│Execute│ │Execute│ │Execute│ │Execute│
│ Tasks │ │ Tasks │ │ Tasks │ │ Tasks │
└───────┘ └──────┘ └──────┘ └──────┘
```

### 2. Communication Flow

1. **Registration**: Workers register with master on startup
2. **Heartbeats**: Workers send heartbeats every 3 seconds
3. **Task Assignment**: Workers request tasks from master
4. **Execution**: Workers execute map tasks (word count)
5. **Reporting**: Workers report results back to master

### 3. Fault Tolerance Mechanism

- **Detection**: Master checks heartbeats every 5 seconds
- **Timeout**: If no heartbeat for 10 seconds → worker marked FAILED
- **Recovery**: Failed worker's tasks automatically reassigned
- **Retry**: Failed tasks retry up to 3 times

### 4. Current Processing Logic

**Simple Word Count Example**:
- Input: `["hello world", "world of mapreduce"]`
- Map Task 1: `{"hello": 1, "world": 1}`
- Map Task 2: `{"world": 1, "of": 1, "mapreduce": 1}`

## Development Commands

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f master
docker-compose logs -f worker1
```

### Check Status
```bash
./scripts/run_cluster.sh status
```

### Restart Cluster
```bash
./scripts/run_cluster.sh restart
```

### Stop Cluster
```bash
./scripts/run_cluster.sh stop
```

### Rebuild After Code Changes
```bash
./scripts/run_cluster.sh rebuild
```

## Local Development (Without Docker)
Note: To test task reassignment to workers, you'll need to kill a terminal as early as possible before it assigns all tasks to one worker (it's so fast)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Protobuf Code
```bash
chmod +x scripts/generate_proto.sh
./scripts/generate_proto.sh
```

### 3. Start Master
```bash
python -m src.master.server
```

### 4. Start Workers (in separate terminals)
```bash
python -m src.worker.client worker1
python -m src.worker.client worker2
python -m src.worker.client worker3
```

### 5. Simulate Worker Failure
Start a worker that fails after N tasks:
```bash
python -m src.worker.client worker_unstable 2
# This worker will fail after completing 2 tasks
```

## Next Steps (Week 8-10)

- [ ] Implement reduce phase
- [ ] Add shuffle logic for intermediate data
- [ ] Implement partitioning across reduce workers
- [ ] Support larger datasets from files

## Troubleshooting

### Workers not connecting
```bash
# Check if master is running
docker-compose ps master

# Restart the cluster
./scripts/run_cluster.sh restart
```

### No tasks being processed
```bash
# Check master logs for task creation
docker-compose logs master | grep "Created"

# Verify workers are registered
docker-compose logs master | grep "registered"
```

### Protobuf import errors
```bash
# Regenerate protobuf code
./scripts/generate_proto.sh
```

## Key Files

- **`protos/mapreduce.proto`**: gRPC service definitions
- **`src/master/server.py`**: Master coordination logic
- **`src/master/monitor.py`**: Fault detection mechanism
- **`src/worker/client.py`**: Worker implementation
- **`docker-compose.yml`**: Multi-container orchestration

## Metrics Tracked

- Workers registered/failed
- Tasks pending/running/completed/failed
- Tasks per worker
- Heartbeat status
- Task reassignment on failure