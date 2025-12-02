import grpc
import time
import threading
import json
import sys
import os
from datetime import datetime

# Add proto path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'protos'))
import mapreduce_pb2
import mapreduce_pb2_grpc

from .executor import TaskExecutor


class Worker:
    """Worker node that executes map/reduce tasks.
    
    Based on Google MapReduce paper:
    - Workers register with master and send periodic heartbeats
    - Workers request tasks from master
    - Map tasks produce intermediate files partitioned by reduce key
    - Reduce tasks fetch intermediate data, shuffle, and reduce
    """
    
    def __init__(self, worker_id, master_address=None, fail_after=None):
        self.worker_id = worker_id
        env_addr = os.environ.get('MASTER_ADDRESS')
        if master_address:
            self.master_address = master_address
        elif env_addr:
            self.master_address = env_addr
        else:
            self.master_address = 'localhost:50051'
        
        self.running = True
        self.fail_after = fail_after  # For testing fault tolerance
        self.tasks_completed = 0
        self.current_task = None
        
        # Will be initialized after registration (when we know R)
        self.num_reduce_tasks = 3  # Default, updated from master
        self.executor = None
        
        # Connect to master
        self.channel = grpc.insecure_channel(self.master_address)
        self.stub = mapreduce_pb2_grpc.MasterServiceStub(self.channel)
        
        print(f"[{datetime.now()}] Worker {worker_id} starting...")
    
    def _init_executor(self):
        """Initialize executor after we know num_reduce_tasks."""
        intermediate_dir = f'./intermediate/{self.worker_id}'
        self.executor = TaskExecutor(
            worker_id=self.worker_id,
            num_reduce_tasks=self.num_reduce_tasks,
            intermediate_dir=intermediate_dir
        )
    
    def register(self):
        """Register with master."""
        try:
            response = self.stub.RegisterWorker(
                mapreduce_pb2.WorkerInfo(
                    worker_id=self.worker_id,
                    host='localhost',
                    port=0  # Not accepting connections in this simple version
                )
            )
            print(f"[{datetime.now()}] Worker {self.worker_id} registered: {response.message}")
            
            # Get R from master if provided
            if response.num_reduce_tasks > 0:
                self.num_reduce_tasks = response.num_reduce_tasks
                print(f"[{datetime.now()}] Using {self.num_reduce_tasks} reduce partitions")
            
            # Initialize executor now that we know R
            self._init_executor()
            
            return True
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to register: {e}")
            return False
    
    def send_heartbeat(self):
        """Send periodic heartbeats to master."""
        while self.running:
            try:
                status = 'busy' if self.current_task else 'idle'
                current_tasks = [self.current_task] if self.current_task else []
                
                self.stub.SendHeartbeat(
                    mapreduce_pb2.Heartbeat(
                        worker_id=self.worker_id,
                        timestamp=int(time.time()),
                        status=status,
                        current_tasks=current_tasks
                    )
                )
            except grpc.RpcError as e:
                print(f"[{datetime.now()}] Heartbeat failed: {e}")
            
            time.sleep(3)  # Send heartbeat every 3 seconds
    
    def request_task(self):
        """Request a task from master."""
        try:
            task = self.stub.RequestTask(
                mapreduce_pb2.TaskRequest(worker_id=self.worker_id)
            )
            
            if task.has_task:
                return task
            return None
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to request task: {e}")
            return None
    
    def report_result(self, task_id, success, output_data='', error_message='', sequence_number=0):
        """Report task completion to master."""
        try:
            response = self.stub.ReportTaskComplete(
                mapreduce_pb2.TaskResult(
                    worker_id=self.worker_id,
                    task_id=task_id,
                    success=success,
                    output_data=output_data,
                    error_message=error_message,
                    task_sequence_number=sequence_number
                )
            )
            return response
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to report result: {e}")
            return None
    
    def report_intermediate_files(self, task_id, file_paths):
        """Report intermediate file locations to master after map task.
        
        Args:
            task_id: ID of completed map task
            file_paths: dict {partition_id: file_path}
        """
        try:
            locations = [
                mapreduce_pb2.FileLocation(
                    partition_id=partition_id,
                    file_path=file_path,
                    file_size=os.path.getsize(file_path) if os.path.exists(file_path) else 0
                )
                for partition_id, file_path in file_paths.items()
            ]
            
            response = self.stub.ReportIntermediateFiles(
                mapreduce_pb2.IntermediateFileInfo(
                    worker_id=self.worker_id,
                    task_id=task_id,
                    locations=locations
                )
            )
            return response
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to report intermediate files: {e}")
            return None
    
    def execute_task(self, task):
        """Execute a map or reduce task."""
        task_id = task.task_id
        task_type = task.task_type
        self.current_task = task_id
        
        print(f"[{datetime.now()}] Worker {self.worker_id} executing {task_type} task {task_id}")
        
        # Update executor's R value if needed
        if task.num_reduce_tasks > 0:
            self.executor.update_num_reduce_tasks(task.num_reduce_tasks)
        
        try:
            if task_type == 'map':
                # Execute map task
                file_paths = self.executor.execute_map(
                    task_id=task_id,
                    input_data=task.input_data
                )
                
                # Report intermediate file locations to master
                self.report_intermediate_files(task_id, file_paths)
                
                # Output for task completion is the file paths
                output = json.dumps(file_paths)
                
            elif task_type == 'reduce':
                # Execute reduce task
                # input_data contains intermediate file locations as JSON
                result = self.executor.execute_reduce(
                    task_id=task_id,
                    partition_id=task.partition_id,
                    intermediate_locations=task.input_data
                )
                output = json.dumps(result)
            else:
                raise ValueError(f"Unknown task type: {task_type}")
            
            # Report success
            self.report_result(
                task_id, 
                True, 
                output,
                sequence_number=task.task_sequence_number
            )
            self.tasks_completed += 1
            
            print(f"[{datetime.now()}] Worker {self.worker_id} completed {task_type} task {task_id}")
            
        except Exception as e:
            print(f"[{datetime.now()}] Worker {self.worker_id} failed task {task_id}: {e}")
            import traceback
            traceback.print_exc()
            self.report_result(
                task_id, 
                False, 
                error_message=str(e),
                sequence_number=task.task_sequence_number
            )
        finally:
            self.current_task = None
    
    def run(self):
        """Main worker loop."""
        # Register with master
        if not self.register():
            print(f"[{datetime.now()}] Worker {self.worker_id} failed to register. Exiting.")
            return
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
        
        # Main task loop
        print(f"[{datetime.now()}] Worker {self.worker_id} ready for tasks")
        
        while self.running:
            # Check if we should simulate failure
            if self.fail_after and self.tasks_completed >= self.fail_after:
                print(f"[{datetime.now()}] 💀 Worker {self.worker_id} SIMULATING FAILURE")
                print(f"[{datetime.now()}] Worker {self.worker_id} completed {self.tasks_completed} tasks before failing")
                break
            
            # Request task
            task = self.request_task()
            
            if task:
                self.execute_task(task)
            else:
                # No tasks available, wait
                time.sleep(2)
        
        print(f"[{datetime.now()}] Worker {self.worker_id} shutting down")


def start_worker(worker_id, master_address=None, fail_after=None):
    """Start a worker process."""
    worker = Worker(worker_id, master_address, fail_after)
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] Worker {worker_id} interrupted")
        worker.running = False


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python client.py <worker_id> [fail_after_n_tasks]")
        sys.exit(1)
    
    worker_id = sys.argv[1]
    fail_after = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    start_worker(worker_id, fail_after=fail_after)