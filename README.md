# MapReduce (learning project)

A minimal MapReduce implementation (master + workers) used for learning and demos. The repo contains a read-only web dashboard for monitoring, gRPC-based master/worker communication, and example scripts for demos and benchmarks.

For a deeper narrative and design notes, see `docs/WRITEUP.md`.

## Quick demo (recommended)

Prerequisites
- Docker & Docker Compose

Start the demo (master + 3 workers + dashboard)

```bash
docker-compose -f docker-compose.benchmark.yml up --build
```

Open the dashboard at: http://localhost:5000

Stop the demo

```bash
docker-compose -f docker-compose.benchmark.yml down
```

## Automated benchmark

Run automated benchmarks (no dashboard):

```bash
python3 examples/benchmark.py
```

This runs three tests: 1-worker baseline, 3-worker parallel run, and a failure-recovery test. Results are printed and saved to `benchmark_results.txt`.

## Project layout (short)

- `protos/` — gRPC `.proto` definitions
- `src/master/` — master server, scheduler, and state management
- `src/worker/` — worker client and task executor
- `web/` — Flask-based read-only dashboard and templates
- `examples/` — demo and benchmark scripts
- `docker-compose*.yml` — compose files for demo, benchmark, and DAG runs
- `docs/WRITEUP.md` — detailed writeup and design notes

## How it works (TL;DR)

1. Master splits the input into map tasks.
2. Workers register and request tasks from the master via gRPC.
3. Mappers write partitioned intermediate files (one file per reducer partition).
4. Reducers fetch intermediate files for their partition (the shuffle) and produce final output.
5. The master exposes `/status` which the dashboard polls for live updates.

Key points
- The web dashboard is read-only and monitors the master's `/status` endpoint.
- There is no master failover implemented (master is a single point of failure).
- Tasks use sequence numbers and completion tracking to tolerate duplicate reports from slow or restarted workers.

## Development (without Docker)

Install dependencies and generate protobufs

```bash
pip install -r requirements.txt
./scripts/generate_proto.sh
```

Start master

```bash
python -m src.master.server --input data/input/sample.txt
```

Start a worker (in another terminal)

```bash
python -m src.worker.client worker1
```

## Useful commands

- Demo: `docker-compose -f docker-compose.benchmark.yml up --build`
- Stop demo: `docker-compose -f docker-compose.benchmark.yml down`
- Run benchmarks: `python3 examples/benchmark.py`
- Regenerate protobufs: `./scripts/generate_proto.sh`

## Simple manual run (no benchmark)

Follow these steps to run the master and workers manually in separate terminals — useful for demoing and stopping individual workers by closing their terminal.

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Generate Protobuf code

```bash
chmod +x scripts/generate_proto.sh
./scripts/generate_proto.sh
```

3. Start Master (new terminal)

```bash
python -m src.master.server
```

4. Start Workers (each in its own terminal; you can stop any worker by closing that terminal)

```bash
python -m src.worker.client worker1
python -m src.worker.client worker2
python -m src.worker.client worker3
```

5. Simulate Worker Failure (unstable worker)

Start a worker that purposely fails after completing N tasks (example below starts one that fails after 2 tasks):

```bash
python -m src.worker.client worker_unstable 2
# This worker will exit/fail after finishing 2 tasks; watch master logs for reassignment
```

Notes
- Stopping a worker terminal (or killing the process) will simulate a crash and the master will detect failure via heartbeats and reassign tasks.
- The `examples/benchmark.py` script automates bringing up/down workers for repeatable tests; use the manual steps above for ad-hoc experimentation.

## Notes & limitations

- Master failover is not implemented — the master is a single point of failure.
- The dashboard is monitoring-only and does not expose control endpoints.
- This project is intended for learning and small-scale demos, not production use.

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
- **`web/app.py`**: Flask web dashboard backend
- **`web/templates/index.html`**: Dashboard UI
- **`docker-compose.yml`**: Basic multi-container orchestration
- **`docker-compose.benchmark.yml`**: Full demo with web dashboard

## Metrics Tracked

- Workers registered/failed
- Tasks pending/running/completed/failed
- Tasks per worker
- Heartbeat status
- Task reassignment on failure