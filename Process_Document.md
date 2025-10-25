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