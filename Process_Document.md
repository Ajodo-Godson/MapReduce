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