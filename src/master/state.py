import threading
import time
from datetime import datetime


class MasterState:
    """Manages the state of workers and tasks with idempotency support"""
    
    def __init__(self):
        self.workers = {}  # worker_id -> worker_info
        self.tasks = {}    # task_id -> task_info
        self.task_sequence = 0  # Monotonically increasing sequence number
        self.completed_sequences = set()  # Track completed task sequences for idempotency
        self.intermediate_files = {}  # task_id -> {partition_id -> file_location}
        self.lock = threading.Lock()
    
    def add_worker(self, worker_id, worker_info):
        """Register a new worker"""
        with self.lock:
            self.workers[worker_id] = {
                **worker_info,
                'registered_at': time.time(),
                'last_heartbeat': time.time(),
                'tasks_completed': 0,
                'tasks_failed': 0,
                'current_tasks': []
            }
    
    def update_heartbeat(self, worker_id, status, current_tasks=None):
        """Update worker heartbeat"""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]['last_heartbeat'] = time.time()
                self.workers[worker_id]['status'] = status
                if current_tasks is not None:
                    self.workers[worker_id]['current_tasks'] = current_tasks
    
    def mark_worker_failed(self, worker_id):
        """Mark a worker as failed"""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]['status'] = 'failed'
                print(f"[{datetime.now()}] Worker {worker_id} marked as FAILED")
    
    def get_worker(self, worker_id):
        """Get worker info"""
        with self.lock:
            return self.workers.get(worker_id, {}).copy()
    
    def get_all_workers(self):
        """Get all workers"""
        with self.lock:
            return {
                wid: {
                    'status': info['status'],
                    'last_heartbeat': time.strftime('%H:%M:%S', time.localtime(info['last_heartbeat'])),
                    'tasks_completed': info.get('tasks_completed', 0),
                    'tasks_failed': info.get('tasks_failed', 0),
                    'current_tasks': info.get('current_tasks', [])
                }
                for wid, info in self.workers.items()
            }
    
    def add_task(self, task_id, task_info):
        """Add a new task"""
        with self.lock:
            self.task_sequence += 1
            self.tasks[task_id] = {
                **task_info,
                'status': 'pending',  # pending, running, completed, failed
                'assigned_worker': None,
                'created_at': time.time(),
                'started_at': None,
                'completed_at': None,
                'attempts': 0,
                'sequence_number': self.task_sequence
            }
    
    def assign_task(self, task_id, worker_id):
        """Assign a task to a worker"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'running'
                self.tasks[task_id]['assigned_worker'] = worker_id
                self.tasks[task_id]['started_at'] = time.time()
                self.tasks[task_id]['attempts'] += 1
                
                # Add to worker's current tasks
                if worker_id in self.workers:
                    self.workers[worker_id]['current_tasks'].append(task_id)
    
    def complete_task(self, task_id, sequence_number, output_data):
        """Mark task as completed (idempotent)"""
        with self.lock:
            # Check if we've already completed this sequence number
            if sequence_number in self.completed_sequences:
                print(f"[{datetime.now()}] Ignoring duplicate completion for task {task_id} (seq {sequence_number})")
                return True  # Is duplicate
            
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['completed_at'] = time.time()
                self.tasks[task_id]['output_data'] = output_data
                
                # Mark sequence as completed
                self.completed_sequences.add(sequence_number)
                
                # Update worker stats
                worker_id = self.tasks[task_id]['assigned_worker']
                if worker_id and worker_id in self.workers:
                    self.workers[worker_id]['tasks_completed'] += 1
                    # Remove from current tasks
                    if task_id in self.workers[worker_id]['current_tasks']:
                        self.workers[worker_id]['current_tasks'].remove(task_id)
                
                return False  # Not a duplicate
    
    def fail_task(self, task_id):
        """Mark task as failed and reset for retry"""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'failed'
                
                # Update worker stats
                worker_id = self.tasks[task_id]['assigned_worker']
                if worker_id and worker_id in self.workers:
                    self.workers[worker_id]['tasks_failed'] += 1
                    # Remove from current tasks
                    if task_id in self.workers[worker_id]['current_tasks']:
                        self.workers[worker_id]['current_tasks'].remove(task_id)
                
                # Reset for potential retry
                if self.tasks[task_id]['attempts'] < 3:  # Max 3 attempts
                    self.tasks[task_id]['status'] = 'pending'
                    self.tasks[task_id]['assigned_worker'] = None
    
    def get_task(self, task_id):
        """Get task info"""
        with self.lock:
            return self.tasks.get(task_id, {}).copy()
    
    def get_all_tasks(self):
        """Get all tasks"""
        with self.lock:
            return self.tasks.copy()
    
    def get_pending_tasks(self):
        """Get all pending tasks"""
        with self.lock:
            return [
                (tid, task) 
                for tid, task in self.tasks.items() 
                if task['status'] == 'pending'
            ]
    
    def reassign_worker_tasks(self, worker_id):
        """Reassign tasks from a failed worker"""
        with self.lock:
            reassigned = []
            for task_id, task in self.tasks.items():
                if task['assigned_worker'] == worker_id and task['status'] == 'running':
                    task['status'] = 'pending'
                    task['assigned_worker'] = None
                    reassigned.append(task_id)
            
            # Clear worker's current tasks
            if worker_id in self.workers:
                self.workers[worker_id]['current_tasks'] = []
            
            if reassigned:
                print(f"[{datetime.now()}] Reassigned {len(reassigned)} tasks from failed worker {worker_id}: {reassigned}")
            
            return reassigned
    
    def store_intermediate_files(self, task_id, file_locations):
        """Store intermediate file locations for a completed map task.
        
        Args:
            task_id: ID of the map task
            file_locations: dict {partition_id: file_path}
        """
        with self.lock:
            # Store locations with reference to which task/worker produced them
            worker_id = None
            if task_id in self.tasks:
                worker_id = self.tasks[task_id].get('assigned_worker')
            
            self.intermediate_files[task_id] = {
                'worker_id': worker_id,
                'locations': file_locations
            }
    
    def get_intermediate_files_for_partition(self, partition_id):
        """Get all intermediate file locations for a specific reduce partition.
        
        Returns list of dicts with file_path, worker_id, task_id for this partition.
        """
        with self.lock:
            locations = []
            for task_id, file_info in self.intermediate_files.items():
                file_locs = file_info.get('locations', {})
                worker_id = file_info.get('worker_id')
                
                if partition_id in file_locs:
                    locations.append({
                        'task_id': task_id,
                        'worker_id': worker_id,
                        'file_path': file_locs[partition_id]
                    })
            return locations