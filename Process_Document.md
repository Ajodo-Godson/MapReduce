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

Rough draft: I don't know what I was thinking here exactly but my initial approach to this was based on having the reducer and mapper scripts working, but I changed the plan to follow the milestone I'd defined earlier. 

So a more formal justification for the current design is: 
1. The abstraction from the paper alone wasn't enough to come up with a perfect design but I kep the same MapReduce concepts like map/shuffle/reduce, task lifecycle in mind, while leveraging python for faster iteration and thank goodness for gRPC/protos that I discovered above. 
2. The gRPC + Protocol Buffers for RPC and serialization helped provide a compact format. Especially since it allow workers call master RPCs (Register, RequestTask, ReportTaskComplete, SendHearteat) as if local functions. 
3. I tested the scripts I have locally on my machine by opening multiple terminal and running them. But I decided Docker was a great way to simulate the networked cluster (I think I mentioned this few writeups above) by spinning up multiple container models to separate machines and let the gRPC networking behavior (service-name DNS ports) be exercised. 
4. Back on the Master state model: We're storing the task state (pending/running/completed), assigned worker, timestamps and attempt counts which might be helpful later in the scheduler I'm yet to finalize on. 
5. Heartbeat-based failure detection and task reassignments: This was one of the things that I thought would be easy to do but somehow it proved challenging. Or to be specific, threading and concurrency was a pain for me. Instead of concurrent operations to help with reassigning tasks when a worker failed(not sending heartbeats), it identifies the failure but doesn't reassign tasks. A fix I made to that was to call the function outside the lock. Somehow it worked, but I don't know why (I'll figure this out later). reassign_worker_tasks resets non-completed tasks to pending so other workers can pick them up. This implements the fault-tolerance behaviour described in the paper. I have a big confusion from the paper: 
"""
Any map tasks completed by the worker are reset back to their initial idle state, and therefore become eligible for scheduling on other workers. Similarly, any map task or reduce
task in progress on a failed worker is also reset to idle
and becomes eligible for rescheduling.
Completed map tasks are re-executed on a failure because their output is stored on the local disk(s) of the failed machine and is therefore inaccessible. Completed reduce tasks do not need to be re-executed since their output is stored in a global file system
"""
According to the quote above, even completed tasks are to be reset to their initial idle state. The reason was the storage on the local disk(s) of the failed machine. I still don't fully understand the logic but as the tasks are completed, the feedback are sent back to the master/leader, including the result of the tasks executed. Does resetting to idle depends on the period between completing a task and the feedback sent to the master? 
So I decided to reset only running tasks from a failed worker by changing it to pending. 
6. The monitoring part: The display was mostly done by AI but the core logic was a collaboration. 
7. Forgot to mention this, we have a very small map and reduce operation here. All it does is return the individual maps and reduce is to just output them. 


I have more tasks to do....... and I'm not a fan of writing Readmes. Lot of markdown formatting :(