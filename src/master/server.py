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
from .checkpoint import MasterCheckpoint


class MasterServicer(mapreduce_pb2_grpc.MasterServiceServicer):
    """Master node that coordinates MapReduce job execution.
    
    Based on Google MapReduce paper:
    - Assigns map tasks to workers
    - Tracks intermediate file locations
    - Creates reduce tasks after map phase completes
    - Handles worker failures and task reassignment
    """
    
    def __init__(self, num_reduce_tasks=3):
        self.num_reduce_tasks = num_reduce_tasks
        self.state = MasterState()
        self.scheduler = TaskScheduler(self.state, num_reduce_tasks=num_reduce_tasks)
        self.monitor = WorkerMonitor(self.state)
        self.checkpoint = MasterCheckpoint()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor.run, daemon=True)
        self.monitor_thread.start()
        
        # Start checkpointing thread
        self.checkpoint.start_periodic_checkpointing(self.state)
        
        print(f"[{datetime.now()}] Master initialized with R={num_reduce_tasks}")
    
    def RegisterWorker(self, request, context):
        """Register a new worker."""
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
            message=f"Worker {worker_id} registered successfully",
            num_reduce_tasks=self.num_reduce_tasks
        )
    
    def SendHeartbeat(self, request, context):
        """Receive heartbeat from worker."""
        worker_id = request.worker_id
        current_tasks = list(request.current_tasks) if request.current_tasks else None
        self.state.update_heartbeat(worker_id, request.status, current_tasks)
        
        return mapreduce_pb2.HeartbeatResponse(acknowledged=True)
    
    def RequestTask(self, request, context):
        """Assign a task to a worker."""
        worker_id = request.worker_id
        task = self.scheduler.get_next_task(worker_id)
        
        if task:
            return mapreduce_pb2.TaskAssignment(
                task_id=task['task_id'],
                task_type=task['task_type'],
                input_data=task['input_data'],
                partition_id=task['partition_id'],
                num_reduce_tasks=task.get('num_reduce_tasks', self.num_reduce_tasks),
                has_task=True,
                task_sequence_number=task.get('sequence_number', 0)
            )
        else:
            return mapreduce_pb2.TaskAssignment(has_task=False)
    
    def ReportTaskComplete(self, request, context):
        """Receive task completion from worker."""
        worker_id = request.worker_id
        task_id = request.task_id
        sequence_number = request.task_sequence_number
        
        if request.success:
            is_duplicate = self.scheduler.mark_task_complete(
                task_id, 
                worker_id, 
                request.output_data,
                sequence_number=sequence_number
            )
            if is_duplicate:
                print(f"[{datetime.now()}] Duplicate completion for task {task_id} (ignored)")
            else:
                print(f"[{datetime.now()}] Task {task_id} completed by {worker_id}")
            
            return mapreduce_pb2.TaskAck(acknowledged=True, is_duplicate=is_duplicate)
        else:
            self.scheduler.mark_task_failed(task_id, worker_id)
            print(f"[{datetime.now()}] Task {task_id} failed on {worker_id}: {request.error_message}")
            return mapreduce_pb2.TaskAck(acknowledged=True, is_duplicate=False)
    
    def ReportIntermediateFiles(self, request, context):
        """Store intermediate file locations from completed map task.
        
        Per Google paper: Master stores locations and sizes of R intermediate
        file regions produced by each map task. This info is pushed to workers
        with in-progress reduce tasks.
        """
        task_id = request.task_id
        worker_id = request.worker_id
        
        # Convert to dict: {partition_id: file_path}
        file_locs = {
            loc.partition_id: loc.file_path 
            for loc in request.locations
        }
        
        self.state.store_intermediate_files(task_id, file_locs)
        
        print(f"[{datetime.now()}] Stored {len(file_locs)} intermediate file locations for {task_id}")
        
        return mapreduce_pb2.TaskAck(acknowledged=True)
    
    def GetIntermediateLocations(self, request, context):
        """Return all intermediate file locations for a reduce partition.
        
        Called by reduce workers to know where to fetch intermediate data.
        """
        partition_id = request.partition_id
        locations = self.state.get_intermediate_files_for_partition(partition_id)
        
        # Build response with worker locations
        worker_locations = []
        for loc in locations:
            worker_info = self.state.get_worker(loc.get('worker_id', ''))
            worker_locations.append(
                mapreduce_pb2.WorkerFileLocation(
                    worker_id=loc.get('worker_id', ''),
                    host=worker_info.get('host', 'localhost'),
                    port=worker_info.get('port', 0),
                    file_path=loc.get('file_path', '')
                )
            )
        
        return mapreduce_pb2.IntermediateLocations(
            partition_id=partition_id,
            locations=worker_locations
        )
    
    def get_status(self):
        """Get current system status for visualization."""
        progress = self.scheduler.get_job_progress()
        return {
            'workers': self.state.get_all_workers(),
            'tasks': self.state.get_all_tasks(),
            'progress': progress,
            'timestamp': datetime.now().isoformat()
        }


def serve(port=50051, input_data=None, num_reduce_tasks=3, use_ascii_viz=True):
    """Start the master server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MasterServicer(num_reduce_tasks=num_reduce_tasks)
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
        job_complete_announced = False
        while True:
            time.sleep(10)
            
            if use_ascii_viz:
                # Use the fancy ASCII visualization
                print("\n" + servicer.state.get_ascii_visualization())
            else:
                # Use simple text status
                status = servicer.get_status()
                progress = status.get('progress', {})
                
                print(f"\n{'='*60}")
                print(f"SYSTEM STATUS - {status['timestamp']}")
                print(f"{'='*60}")
                print(f"Workers: {len(status['workers'])} registered")
                for wid, info in status['workers'].items():
                    tasks_str = f", tasks: {info.get('current_tasks', [])}" if info.get('current_tasks') else ""
                    print(f"  {wid}: {info['status']} (last seen: {info.get('last_heartbeat', 'never')}{tasks_str})")
                
                print(f"\nMap Phase: {'✅ Complete' if progress.get('map_phase_complete') else '🔄 In Progress'}")
                print(f"  Total: {progress.get('map_total', 0)} | "
                      f"Completed: {progress.get('map_completed', 0)} | "
                      f"Running: {progress.get('map_running', 0)} | "
                      f"Pending: {progress.get('map_pending', 0)}")
                
                print(f"\nReduce Phase:")
                print(f"  Total: {progress.get('reduce_total', 0)} | "
                      f"Completed: {progress.get('reduce_completed', 0)} | "
                      f"Running: {progress.get('reduce_running', 0)} | "
                      f"Pending: {progress.get('reduce_pending', 0)}")
                
                print(f"{'='*60}\n")
            
            # Check for job completion (only announce once)
            progress = servicer.scheduler.get_job_progress()
            if progress.get('job_complete') and not job_complete_announced:
                print("\n" + "🎉" * 20)
                print("       MAPREDUCE JOB COMPLETE!")
                print("🎉" * 20 + "\n")
                job_complete_announced = True
    
    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] Shutting down master server...")
        servicer.checkpoint.stop()
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
    serve(input_data=sample_data, num_reduce_tasks=3)