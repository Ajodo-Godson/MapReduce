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