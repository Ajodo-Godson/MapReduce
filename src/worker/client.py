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
    """Worker node that executes map/reduce tasks"""
    
    def __init__(self, worker_id, master_address=None, fail_after=None):
        self.worker_id = worker_id
        env_addr = os.environ.get('MASTER_ADDRESS')
        if master_address:
            self.master_address = master_address
        elif env_addr:
            self.master_address = env_addr
        else:
            self.master_address = 'localhost:50051'
        self.executor = TaskExecutor()
        self.running = True
        self.fail_after = fail_after  # For testing fault tolerance
        self.tasks_completed = 0
        
        # Connect to master
        self.channel = grpc.insecure_channel(self.master_address)
        self.stub = mapreduce_pb2_grpc.MasterServiceStub(self.channel)
        
        print(f"[{datetime.now()}] Worker {worker_id} starting...")
    
    def register(self):
        """Register with master"""
        try:
            response = self.stub.RegisterWorker(
                mapreduce_pb2.WorkerInfo(
                    worker_id=self.worker_id,
                    host='localhost',
                    port=0  # Not accepting connections in this simple version
                )
            )
            print(f"[{datetime.now()}] Worker {self.worker_id} registered: {response.message}")
            return True
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to register: {e}")
            return False
    
    def send_heartbeat(self):
        """Send periodic heartbeats to master"""
        while self.running:
            try:
                status = 'idle' if self.tasks_completed == 0 else 'busy'
                self.stub.SendHeartbeat(
                    mapreduce_pb2.Heartbeat(
                        worker_id=self.worker_id,
                        timestamp=int(time.time()),
                        status=status
                    )
                )
            except grpc.RpcError as e:
                print(f"[{datetime.now()}] Heartbeat failed: {e}")
            
            time.sleep(3)  # Send heartbeat every 3 seconds
    
    def request_task(self):
        """Request a task from master"""
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
    
    def report_result(self, task_id, success, output_data='', error_message=''):
        """Report task completion to master"""
        try:
            self.stub.ReportTaskComplete(
                mapreduce_pb2.TaskResult(
                    worker_id=self.worker_id,
                    task_id=task_id,
                    success=success,
                    output_data=output_data,
                    error_message=error_message
                )
            )
        except grpc.RpcError as e:
            print(f"[{datetime.now()}] Failed to report result: {e}")
    
    def execute_task(self, task):
        """Execute a map or reduce task"""
        task_id = task.task_id
        task_type = task.task_type
        
        print(f"[{datetime.now()}] Worker {self.worker_id} executing {task_type} task {task_id}")
        
        # Simulate work
        time.sleep(2)
        
        try:
            if task_type == 'map':
                result = self.executor.execute_map(task.input_data)
            else:
                result = self.executor.execute_reduce(task.input_data)
            
            output = json.dumps(result)
            self.report_result(task_id, True, output)
            self.tasks_completed += 1
            
            print(f"[{datetime.now()}] Worker {self.worker_id} completed task {task_id}")
            
        except Exception as e:
            print(f"[{datetime.now()}] Worker {self.worker_id} failed task {task_id}: {e}")
            self.report_result(task_id, False, error_message=str(e))
    
    def run(self):
        """Main worker loop"""
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
    """Start a worker process"""
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