"""
MapReduce Master Server

This is the main master node that:
1. Accepts worker registrations
2. Assigns map/reduce tasks to workers
3. Monitors worker health
4. Handles fault tolerance
5. Supports DAG-based job pipelines (multiple chained jobs)
6. Exposes HTTP status endpoint for dashboard
"""

import grpc
from concurrent import futures
import time
import json
import threading
from datetime import datetime
from pathlib import Path
import sys
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add proto path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'protos'))
import mapreduce_pb2
import mapreduce_pb2_grpc

from .state import MasterState
from .scheduler import TaskScheduler
from .monitor import WorkerMonitor
from .checkpoint import MasterCheckpoint
from .job_scheduler import DAGJobScheduler, JobStatus


class MasterServicer(mapreduce_pb2_grpc.MasterServiceServicer):
    """Master node that coordinates MapReduce job execution.
    
    Based on Google MapReduce paper:
    - Assigns map tasks to workers
    - Tracks intermediate file locations
    - Creates reduce tasks after map phase completes
    - Handles worker failures and task reassignment
    - Supports DAG pipelines with job chaining
    """
    
    def __init__(self, num_reduce_tasks=3, pipeline_mode=False):
        self.num_reduce_tasks = num_reduce_tasks
        self.pipeline_mode = pipeline_mode
        self.state = MasterState()
        self.scheduler = TaskScheduler(self.state, num_reduce_tasks=num_reduce_tasks)
        self.monitor = WorkerMonitor(self.state)
        self.checkpoint = MasterCheckpoint()
        
        # DAG job scheduler (used in pipeline mode)
        self.dag_scheduler = DAGJobScheduler() if pipeline_mode else None
        self.current_job_id = None
        self.job_outputs = {}  # job_id -> output_path
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor.run, daemon=True)
        self.monitor_thread.start()
        
        # Start checkpointing thread
        self.checkpoint.start_periodic_checkpointing(self.state)
        
        mode_str = "pipeline" if pipeline_mode else "single-job"
        print(f"[{datetime.now()}] Master initialized with R={num_reduce_tasks} ({mode_str} mode)")
    
    
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
        """Handle Worker heartbeat."""
        worker_id = request.worker_id
        status = request.status

        with self.state.lock:
            if worker_id in  self.state.workers:
                self.state.workers[worker_id]['last_heartbeat'] = time.time()
                self.state.workers[worker_id]['status'] = status
                self.state.workers[worker_id]['current_tasks'] = list(request.current_tasks) if request.current_tasks else []

        

        return mapreduce_pb2.HeartbeatAck(acknowledged=True)
    
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
            
            # In pipeline mode, check if current job is complete
            if self.pipeline_mode and not is_duplicate:
                self._check_job_completion()
            
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
    
    # ========== DAG Pipeline Methods ==========
    
    def _check_job_completion(self):
        """Check if the current job is complete and start the next one."""
        if not self.current_job_id or not self.dag_scheduler:
            return
        
        progress = self.scheduler.get_job_progress()
        if progress.get('job_complete'):
            # Save job output
            output_path = self._save_job_output(self.current_job_id)
            self.job_outputs[self.current_job_id] = output_path
            
            # Mark job as complete in DAG scheduler
            self.dag_scheduler.mark_job_completed(
                self.current_job_id, 
                output_data={'output_path': output_path}
            )
            
            print(f"\n[{datetime.now()}] Job '{self.current_job_id}' COMPLETE")
            print(f"  Output saved to: {output_path}")
            
            # Start next job if available
            self._start_next_job()
    
    def _save_job_output(self, job_id):
        """Save the reduce output for a job."""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        output_path = output_dir / f"{job_id}_output.json"
        
        # Collect reduce outputs from state
        reduce_outputs = self.state.get_reduce_outputs()
        
        with open(output_path, 'w') as f:
            json.dump(reduce_outputs, f, indent=2)
        
        return str(output_path)
    
    def _start_next_job(self):
        """Start the next ready job from the DAG."""
        if not self.dag_scheduler:
            return
            
        next_job_id = self.dag_scheduler.get_next_ready_job()
        
        if next_job_id:
            self._run_job(next_job_id)
        elif self.dag_scheduler.is_all_complete():
            print("\n" + "=" * 60)
            print("ALL PIPELINE JOBS COMPLETE!")
            print("=" * 60)
            self._print_pipeline_summary()
    
    def _run_job(self, job_id):
        """Run a single MapReduce job."""
        if not self.dag_scheduler:
            return
            
        job = self.dag_scheduler.jobs.get(job_id)
        if not job:
            return
        
        self.current_job_id = job_id
        self.dag_scheduler.mark_job_started(job_id)
        
        # Reset state for new job
        self.state.reset_for_new_job()
        self.scheduler.reset_for_new_job()
        
        # Determine input data
        input_data = self._resolve_job_input(job)
        
        print(f"\n[{datetime.now()}] Starting job: {job_id}")
        print(f"  Input: {len(input_data)} chunks")
        
        # Create map tasks
        self.scheduler.create_map_tasks(input_data)
    
    def _resolve_job_input(self, job):
        """Resolve the input data for a job, handling dependency outputs."""
        input_source = job.input_data
        
        # If input is from a dependency, load that job's output
        if input_source and input_source.startswith("output/"):
            output_file = input_source
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    data = json.load(f)
                return self._output_to_input(data)
        
        # If no explicit input, check dependencies
        if not input_source and job.dependencies:
            combined_input = []
            for dep_id in job.dependencies:
                if dep_id in self.job_outputs:
                    output_file = self.job_outputs[dep_id]
                    if os.path.exists(output_file):
                        with open(output_file, 'r') as f:
                            data = json.load(f)
                        combined_input.extend(self._output_to_input(data))
            return combined_input
        
        # Load from file path
        if input_source and os.path.exists(input_source):
            return load_input_data(input_source, chunk_size=500)
        
        return []
    
    def _output_to_input(self, output_data):
        """Convert job output to input format for the next job."""
        if isinstance(output_data, dict):
            lines = [f"{word}\t{count}" for word, count in output_data.items()]
            # Chunk the lines
            chunk_size = 100
            chunks = []
            for i in range(0, len(lines), chunk_size):
                chunk = '\n'.join(lines[i:i + chunk_size])
                chunks.append(chunk)
            return chunks
        return [str(output_data)]
    
    def _print_pipeline_summary(self):
        """Print summary of all completed jobs."""
        if not self.dag_scheduler:
            return
            
        print("\nPipeline Summary:")
        for job_id, job in self.dag_scheduler.jobs.items():
            duration = "N/A"
            if job.started_at and job.completed_at:
                duration = f"{(job.completed_at - job.started_at).total_seconds():.2f}s"
            print(f"  {job_id}: {job.status.value} (duration: {duration})")
            if job_id in self.job_outputs:
                print(f"    Output: {self.job_outputs[job_id]}")
    
    def setup_pipeline(self, pipeline_config):
        """Set up a pipeline of jobs from configuration."""
        if not self.dag_scheduler:
            print("Error: Pipeline mode not enabled")
            return False
            
        for job_config in pipeline_config.get('jobs', []):
            job_id = job_config['id']
            input_data = job_config.get('input')
            dependencies = job_config.get('dependencies', [])
            
            self.dag_scheduler.add_job(
                job_id,
                input_data=input_data,
                dependencies=dependencies,
                num_reduce_tasks=job_config.get('reduce_tasks', self.num_reduce_tasks)
            )
        
        # Validate DAG
        try:
            order = self.dag_scheduler.get_topological_order()
            print(f"[{datetime.now()}] Pipeline validated. Execution order: {' -> '.join(order)}")
        except ValueError as e:
            print(f"[{datetime.now()}] ERROR: Invalid pipeline - {e}")
            return False
        
        return True
    
    def start_pipeline(self):
        """Start executing the pipeline."""
        if not self.dag_scheduler:
            print("Error: Pipeline mode not enabled")
            return
            
        print("\n" + "=" * 60)
        print("STARTING PIPELINE")
        print("=" * 60)
        print(self.dag_scheduler.get_ascii_dag())
        
        self._start_next_job()


def serve(port=50051, input_data=None, num_reduce_tasks=3, use_ascii_viz=True, 
          pipeline_config=None, http_port=8080):
    """Start the master server.
    
    Args:
        port: gRPC server port
        input_data: List of input chunks for single-job mode
        num_reduce_tasks: Number of reduce tasks (R)
        use_ascii_viz: Use ASCII visualization for status
        pipeline_config: Pipeline configuration dict for DAG mode (None = single-job mode)
        http_port: HTTP status endpoint port for dashboard
    """
    pipeline_mode = pipeline_config is not None
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MasterServicer(num_reduce_tasks=num_reduce_tasks, pipeline_mode=pipeline_mode)
    mapreduce_pb2_grpc.add_MasterServiceServicer_to_server(servicer, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"[{datetime.now()}] Master server started on port {port}")
    
    # Start HTTP status server for dashboard
    def create_status_handler(master_servicer):
        class StatusHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass  # Suppress HTTP logging
            
            def do_GET(self):
                if self.path == '/status' or self.path == '/api/status':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    
                    # Get status from master
                    progress = master_servicer.scheduler.get_job_progress()
                    workers = master_servicer.state.get_all_workers()
                    events = master_servicer.state.get_recent_events()
                    
                    status_data = {
                        'workers': {
                            wid: {
                                'status': info.get('status', 'idle'),
                                'current_task': info.get('current_tasks', [''])[0] if info.get('current_tasks') else '',
                                'last_seen': info.get('last_heartbeat', 0)
                            } for wid, info in workers.items()
                        },
                        'tasks': {
                            'map': {
                                'pending': progress.get('map_pending', 0),
                                'running': progress.get('map_running', 0),
                                'completed': progress.get('map_completed', 0),
                                'total': progress.get('map_total', 0)
                            },
                            'reduce': {
                                'pending': progress.get('reduce_pending', 0),
                                'running': progress.get('reduce_running', 0),
                                'completed': progress.get('reduce_completed', 0),
                                'total': progress.get('reduce_total', 0)
                            }
                        },
                        'status': 'completed' if progress.get('job_complete') else 'running' if progress.get('map_total', 0) > 0 else 'idle',
                        'events': events[-20:] if events else [],
                        'last_update': datetime.now().isoformat()
                    }
                    
                    self.wfile.write(json.dumps(status_data).encode())
                else:
                    self.send_response(404)
                    self.end_headers()
        
        return StatusHandler
    
    def run_http_server():
        handler = create_status_handler(servicer)
        http_server = HTTPServer(('0.0.0.0', http_port), handler)
        print(f"[{datetime.now()}] HTTP status server started on port {http_port}")
        http_server.serve_forever()
    
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    if pipeline_mode:
        # Pipeline mode: set up DAG and wait for workers
        if servicer.setup_pipeline(pipeline_config):
            print(f"[{datetime.now()}] Waiting for workers to connect...")
            time.sleep(5)
            servicer.start_pipeline()
    elif input_data:
        # Single-job mode: create map tasks immediately
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
                # In pipeline mode, also show DAG status
                if pipeline_mode and servicer.dag_scheduler:
                    print("\nPipeline Status:")
                    print(servicer.dag_scheduler.get_ascii_dag())
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


def load_input_data(file_path, chunk_size=None):
    """Load and split input file into chunks for map tasks.
    
    Args:
        file_path: Path to the input file
        chunk_size: Number of lines per chunk (None = one line per chunk)
    
    Returns:
        List of text chunks, each becoming one map task
    """
    import os
    
    if not os.path.exists(file_path):
        print(f"Warning: File {file_path} not found")
        return []
    
    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    if chunk_size is None or chunk_size <= 1:
        # One line per map task
        return lines
    
    # Group lines into chunks
    chunks = []
    for i in range(0, len(lines), chunk_size):
        chunk = '\n'.join(lines[i:i + chunk_size])
        chunks.append(chunk)
    
    return chunks


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='MapReduce Master Server')
    parser.add_argument('--input', '-i', type=str, default='data/input/sample.txt',
                        help='Input file path (for single-job mode)')
    parser.add_argument('--pipeline', type=str, default=None,
                        help='Path to pipeline configuration JSON file (for DAG mode)')
    parser.add_argument('--reduce-tasks', '-r', type=int, default=3,
                        help='Number of reduce tasks (R)')
    parser.add_argument('--chunk-size', '-c', type=int, default=1,
                        help='Lines per map task chunk')
    parser.add_argument('--port', '-p', type=int, default=50051,
                        help='Server port')
    
    args = parser.parse_args()
    
    pipeline_config = None
    input_data = None
    
    if args.pipeline:
        # Pipeline mode
        if os.path.exists(args.pipeline):
            with open(args.pipeline, 'r') as f:
                pipeline_config = json.load(f)
            print(f"[{datetime.now()}] Loaded pipeline from {args.pipeline}")
        else:
            print(f"[{datetime.now()}] ERROR: Pipeline file not found: {args.pipeline}")
            sys.exit(1)
    else:
        # Single-job mode
        input_data = load_input_data(args.input, chunk_size=args.chunk_size)
        
        if input_data:
            print(f"[{datetime.now()}] Loaded {len(input_data)} input chunks from {args.input}")
        else:
            print(f"[{datetime.now()}] No input data - workers will wait for tasks")
    
    serve(
        port=args.port, 
        input_data=input_data, 
        num_reduce_tasks=args.reduce_tasks,
        pipeline_config=pipeline_config
    )