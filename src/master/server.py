import grpc
from concurrent import futures
import time
import json
import threading
from datetime import datetime
import sys
import os

# Add proto path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'protos'))
import mapreduce_pb2
import mapreduce_pb2_grpc

from .state import MasterState
from .scheduler import TaskScheduler
from .monitor import WorkerMonitor


class MasterServicer(mapreduce_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        self.state = MasterState()
        self.scheduler = TaskScheduler(self.state)
        self.monitor = WorkerMonitor(self.state)
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor.run, daemon=True)
        self.monitor_thread.start()
        
        print(f"[{datetime.now()}] Master initialized")
    
    def RegisterWorker(self, request, context):
        """Register a new worker"""
        worker_id = request.worker_id
        worker_info = {
            'host': request.host,
            'port': request.port,
            'status': 'idle',
            'last_heartbeat': time.time()
        }
        
        self.state.add_worker(worker_id, worker_info)
        print(f"[{datetime.now()}] Worker registered: {worker_id}")
        
        return mapreduce_pb2.RegisterResponse(
            success=True,
            message=f"Worker {worker_id} registered successfully"
        )
    
    def SendHeartbeat(self, request, context):
        """Receive heartbeat from worker"""
        worker_id = request.worker_id
        self.state.update_heartbeat(worker_id, request.status)
        
        return mapreduce_pb2.HeartbeatResponse(acknowledged=True)
    
    def RequestTask(self, request, context):
        """Assign a task to a worker"""
        worker_id = request.worker_id
        task = self.scheduler.get_next_task(worker_id)
        
        if task:
            return mapreduce_pb2.TaskAssignment(
                task_id=task['task_id'],
                task_type=task['task_type'],
                input_data=task['input_data'],
                partition_id=task['partition_id'],
                has_task=True
            )
        else:
            return mapreduce_pb2.TaskAssignment(has_task=False)
    
    def ReportTaskComplete(self, request, context):
        """Receive task completion from worker"""
        worker_id = request.worker_id
        task_id = request.task_id
        
        if request.success:
            self.scheduler.mark_task_complete(
                task_id, 
                worker_id, 
                request.output_data
            )
            print(f"[{datetime.now()}] Task {task_id} completed by {worker_id}")
        else:
            self.scheduler.mark_task_failed(task_id, worker_id)
            print(f"[{datetime.now()}] Task {task_id} failed on {worker_id}: {request.error_message}")
        
        return mapreduce_pb2.TaskAck(acknowledged=True)
    
    def get_status(self):
        """Get current system status for visualization"""
        return {
            'workers': self.state.get_all_workers(),
            'tasks': self.state.get_all_tasks(),
            'timestamp': datetime.now().isoformat()
        }


def serve(port=50051, input_data=None):
    """Start the master server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MasterServicer()
    mapreduce_pb2_grpc.add_MasterServiceServicer_to_server(servicer, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"[{datetime.now()}] Master server started on port {port}")
    
    # If input data provided, create map tasks
    if input_data:
        servicer.scheduler.create_map_tasks(input_data)
        print(f"[{datetime.now()}] Created {len(input_data)} map tasks")
    
    # Status visualization thread
    def print_status():
        while True:
            time.sleep(10)
            status = servicer.get_status()
            print(f"\n{'='*60}")
            print(f"SYSTEM STATUS - {status['timestamp']}")
            print(f"{'='*60}")
            print(f"Workers: {len(status['workers'])} registered")
            for wid, info in status['workers'].items():
                print(f"  {wid}: {info['status']} (last seen: {info.get('last_heartbeat', 'never')})")
            print(f"\nTasks:")
            print(f"  Pending: {sum(1 for t in status['tasks'].values() if t['status'] == 'pending')}")
            print(f"  Running: {sum(1 for t in status['tasks'].values() if t['status'] == 'running')}")
            print(f"  Completed: {sum(1 for t in status['tasks'].values() if t['status'] == 'completed')}")
            print(f"  Failed: {sum(1 for t in status['tasks'].values() if t['status'] == 'failed')}")
            print(f"{'='*60}\n")
    
    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] Shutting down master server...")
        server.stop(0)


if __name__ == '__main__':
    
    sample_data = [
        "hello world hello",
        "world of mapreduce",
        "hello distributed systems",
        "mapreduce framework design",
        "distributed computing concepts",
        "fault tolerance mechanisms",
        "worker node failures",
        "task reassignment logic"
    ]
    serve(input_data=sample_data)