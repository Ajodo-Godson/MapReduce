The original implementation was done using C++. I'll be abstracting most of the cpp methods and approaches to equivalence in python. 
This includes the basic grpc, Map, Shuffling and Reduce implementations. 

To-Know: 
- How proto files work
- Workings of grpc
- Workers and Client simulated processes and outputs

Important Types: 
The map and reduce functiosn supplied by the users have associated types: 
Map --> (k1, v1) ----> list(k2, v2)
Reduce --> (k2, list(v2)) --> list(v2)

Definitely, our Input KV will be different from the output KVs. So we have to do those conversions. 

Some examples of ezpressable MapReduce computations: 
- Disributed Grep: MAP emits a line if it matches a supplied pattern. REDUCE (Identity function)  copies the intermediate data to the output
- Count of URL Access Frequency : MAP processes web page requess log and outputs {URL, 1}. REDUCE adds all values for the same URL and emits {URL, total count} pair. Similar to word count
- Reverse Web-Link Graph: MAP outputs <target, source> pris for each link to target-URL found in a page named source. REDUCE concatentes the list of all source URLs associated with the given target URL and emits the pair <target, list(source)> 
    and others like: 
- Term-Vector per Host
- Inverted Index
- Distributed Sort

## Implementation Approach
This typically depends on the environment. My focus environment is a collection of networked machines. Other environments could be for a single shared memory or large NUMA multi-processor. Since I don't have too many machines (as in the literal sense), I would spin up multiple docker clusters to simulate the multiple machines for the distributed network. Each worker process can run on a different machine, and the gRPC framework will handle the network communication for task assignment and the "shuffle" phase. So we gain both parallelism and scalability. 

The simulated machine is very different from what's used in the paper: dual-processor x86 processors running LInux, with 2-4GB of memory per machine. Their cluster also consists of thousands of machines, but I will be working with 8 to 10 machines (for now). 


### MASTER DATA STRUCTURES
For each MAP and REDUCE tasks, we store,
- State: - Idle, In-Progress, Completed
- Identity of the Worker Machine
- Location and Sizes of R intermediate file regions produced by the MAP task
- Updates to these locations and sizes information are received as MAP tasks are completed.
- The Information is pushed incrementally to workers that have in-progress reduce tasks. 

### Handling Failure
- Master pings every worker periodically. if no response is received from a worker, the master marks the worker as failed. Map tasks completed/inprogress by the worker are reset back to their initial idle state and become eligible for scheudling on other workers. From the paper, we reset even completed tasks to idle cos outputs are stored on the local disks of the failed machine and inaccessible. 
- Master write periodic checkpoints. If the master taks dies, a new copy can be started from the last checkpointed state. 
A critique of how this was handled in the paper: the implmentation assumes a single master and the unlikeliness of a failure. If the worst case of a failure happens, the mapreduce computation aborts totally. A way I can go about this will be similar to the way Raft works, where we can hold elections and then re-elect a new leader/master from the candidates nodes/machines. 


## PROGRESS
I'm starting off with defining the proto file. 
What's Proto? 
It's a mechanism for serializing structured data. Very similar to XML or JSON but smaller and faster. 
Data serialization is the process of converting complex data structures into a format that can be easily transmitted over a network or stored in a file. 
For communication among the machines, we use gRPC which enables a client application to directly call a method on a server application on a different machine as if it were a local object.
gRPC uses Proto(col Buffers) for serializing structured data. 
Read more on grpc: https://grpc.io/docs/what-is-grpc/introduction/ 


## WORKER and MASTER clients (I forgot to document this when I worked on it)
I started off by defining the Worker class and ensured the core methods are available.
First are the essential objects in the class. I'll define this in form of Struct
{
    worker_id, 
    master_address
    executor --> a function to execute task
    running --> boolean 
    fail_after --> to test fault tolerance
    tasks_completed

    # This defines the connection to the master
    channel 
    stub
}
The Methods we have include: 
- register
- send_heartbeat
- send request_task


## Worker State — thinking & justification

I want the worker state to be minimal, observable, and easy to reset on failure. Workers are ephemeral in my environment (containers or processes can die), so the state lives mostly in memory and is reported often to the master. Keep the state surface small to reduce synchronization complexity and failure modes.

Contract (what the worker state provides):
- Inputs: assigned TaskDescriptor (map or reduce), local execution status and health metrics.
- Outputs: periodic heartbeats and task status updates (started, progress, completed, failed), plus locations of any produced intermediate files.
- Error modes: worker can lose local files on crash; master must be prepared to re-run tasks or fetch shards from alternate places.

Core fields (where they live: `src/worker/client.py`, `src/worker/executor.py`):
- `worker_id` — stable identifier for a worker process instance.
- `running` / `alive` — boolean; used for test harness and clean shutdown.
- `current_task` — the TaskDescriptor currently executing (or `None`). Keep this shallow (id, type, input-split ref, output location hint).
- `task_state` — Enum: Idle, Running, Completed, Failed. This is local view; master is authoritative.
- `progress` — optional numeric progress (0-100) or simple phase markers (mapping, shuffling, reducing).
- `intermediate_locations` — list of (file_path, offset_range) for map outputs that the worker produced. Reported to master immediately on completion.
- `fail_after` / `debug_flags` — dev/test only: used to simulate failures for resilience testing.

Why these choices (design notes):
- Minimal local state: avoid complex, durable local metadata. If the worker dies, re-running is simpler than trying to recover partial state.
- Report intermediate locations, not raw data: avoids pumping large payloads through RPCs; the master and other workers can fetch files via the filesystem/HTTP when needed.
- Master-as-source-of-truth: the worker keeps a local view for speed, but every important transition is reported; this keeps reasoning about scheduling simpler.
- Keep health and progress simple: we don't need fine-grained progress for correctness, just enough to detect stuck tasks and trigger retries.

Edge cases / failure handling:
- Crash during map: intermediate files may be partially written — treat as non-existent and re-run the map task elsewhere.
- Network partition: worker continues local work but can't contact master. It should retry heartbeats with exponential backoff and mark tasks as paused if master remains unreachable for a long time.
- Disk-full on write: return an explicit failure to master with an error code so the scheduler knows to reschedule or mark the worker unhealthy.

Example (how the worker reports completion):
- On map completion, worker writes intermediate files locally, then sends ReportTaskResult containing `task_id`, `status=COMPLETED`, and `intermediate_locations=[(path, start, size), ...]`.
- Master acknowledges and records the locations (but the master may still treat the map as "not durable" until reducers read them or until replication is added).

Trade-offs considered:
- Durable replication vs simplicity: I prefer simplicity for now (no replication of intermediate files). That means master may re-run tasks on failure but the overall system is simpler to build and reason about.
- Push vs pull of intermediate data: pushing large data over RPCs simplifies the master but hurts scalability. I prefer pull (workers expose file locations) to keep RPC payloads small.


## Master State — thinking & justification

The master must be authoritative and durable (to the level implemented). Its state is the key source of truth for scheduling and recovery. Because I want to be able to checkpoint and restart the master (and because the master must make global decisions), state design must balance completeness and ease of checkpointing.

Contract (what the master state provides):
- Inputs: worker registrations, heartbeats, task results, user job submissions.
- Outputs: task assignments, reassignments on failure, global job progress, persistent checkpoints (optional).
- Error modes: master process crash — must be able to restart from the last checkpoint or re-derive a safe state (idempotency matters).

Core fields (where they live: `src/master/state.py`, used by `src/master/scheduler.py` and `src/master/server.py`):
- `jobs` — map of job_id -> JobMetadata (job definition, user map/reduce functions, input splits, requested R).
- `map_tasks` / `reduce_tasks` — tables keyed by task_id describing: state (Idle, In-Progress, Completed), assigned_worker (if any), intermediate locations (for maps), retries count, timestamps (start, last_update).
- `workers` — worker table keyed by worker_id with last_heartbeat, health_state, capability hints (available slots, resources), address/location.
- `intermediate_index` — mapping from map_task -> list(intermediate_locations) so reducers can find inputs. This is critical for shuffle.
- `checkpoints` / `log` — optional write-ahead or checkpoint snapshot to allow recovery after master crash. Even simple periodic pickling is useful here.

Why these choices (design notes):
- Master must be authoritative: even if workers report local state, decisions come from the master; that centralization simplifies correctness reasoning.
- Keep task records small but sufficient: we store locations (not content) for intermediate outputs. This keeps the master memory-light while still enabling shuffle.
- Track retries and timestamps: this enables failure detection and re-scheduling policies (e.g., exponential backoff, max retries).
- Checkpointing: I prefer periodic snapshots of `state.py` to disk. That is easier than building a full consensus system (Raft) early on and covers many common failure modes.

Failure handling and invariants:
- Worker failure: master marks worker as failed if heartbeats expire, then moves any in-progress tasks back to Idle and increments retry counters. Completed map tasks produced only on the failed worker are treated as lost (unless we added replication).
- Master restart: on restart from checkpoint, master must reconcile worker liveness (workers will re-register or heartbeats will repopulate the table). Any tasks marked In-Progress with stale timestamps should be reverted to Idle after a timeout.
- Idempotency: task re-execution must be safe (map/reduce functions should be treated as idempotent or the framework must handle duplicate outputs). This affects how reducers merge partial outputs.

Edge cases / trade-offs:
- Single-master simplicity vs HA complexity: starting with a single master is simple; adding leader election/replicated state (Raft) is a later improvement. The checkpoint approach gives a reasonable middle ground for development.
- Metadata scale: the master stores only metadata (task tables, locations). If the job metadata grows very large, we may need to shard or externalize the metadata store.

Quick notes on implementation points:
- When recording intermediate_locations, record both the file path and a checksum or size to detect partial writes.
- Use monotonically increasing `task_attempt_id` for retries so that stale worker reports can be ignored when a task was already reassigned.
- When master notices repeated failures on a worker, add it to a quarantine list and reduce scheduling to it until admin intervention.


*** End Patch

