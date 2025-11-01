import threading
import time
from datetime import datetime, timedelta

from typing import Dict, List, Optional


class MasterState:
    """ Manages the state of workers and tasks"""
    def __init__(self):
        self.workers = {}  
        self.tasks = {}
        self.lock = threading.Lock()

    def add_worker(self, worker_id, worker_info):
        """Registers a new worker with the master."""
        with self.lock:
            self.workers[worker_id] = {
                **worker_info, 
                'registered_at': time.time(),
                'last_heartbeat': time.time(),
                'tasks_completed': 0,
                'tasks_failed': 0, 
                'tasks_assigned': 0
            }
    def update_heartbeat(self, worker_id, status):
        """Updates the heartbeat information for a worker."""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]['last_heartbeat'] = time.time()
                self.workers[worker_id]['status'] = status
    
    def mark_worker_failed(self, worker_id):
        """Marks a worker as failed."""
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]['status'] = 'failed'
                print(f"[{datetime.now()}] ⚠️  Worker {worker_id} marked as failed")
                # # Reset tasks assigned to this worker including the completed ones
                # for task_id, task in self.tasks.items():
                #     if task['assigned_worker'] == worker_id:
                #         task['status'] = 'idle'
                #         task['assigned_worker'] = None
        self.reassign_worker_tasks(worker_id)
                
    def get_worker(self, worker_id):
        """Retrieves information about a specific worker."""
        with self.lock: 
            return self.workers.get(worker_id, {}).copy()

    def get_all_workers(self):
        """Retrieves information about all workers."""
        with self.lock: 
            return {
                wid: {
                    'status': info['status'],
                    'last_heartbeat': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info['last_heartbeat'])),
                    'tasks_completed': info.get('tasks_completed', 0),
                    'tasks_failed': info.get('tasks_failed', 0)
                }
                for wid, info in self.workers.items()
            }
        

    def add_task(self, task_id, task_info):
        """Adds a new task to the master."""
        with self.lock:
            self.tasks[task_id] = {
                **task_info, 
                'status': 'pending', # pending, running, completed, failed
                'assigned_worker': None,
                'created_at': time.time(), 
                'started_at': None,
                'completed_at': None,
                'attempts': 0
            }
        

    def assign_task(self, task_id, worker_id):
        """Assigns an idle task to a worker."""
        with self.lock:
            if task_id in self.tasks and worker_id in self.workers:
                task = self.tasks[task_id]
                if task['status'] == 'pending':
                    task['assigned_worker'] = worker_id
                    task['status'] = 'running'
                    task['started_at'] = time.time()
                    task['attempts'] += 1
                    self.workers[worker_id]['tasks_assigned'] += 1 # Consistency in ACID here. If we're assining a task, we should increment the assigned count.


    def complete_task(self, task_id, output_data):
        """Marks a task as complete or failed."""
        with self.lock: 
            if task_id in self.tasks:
                task = self.tasks[task_id]
                worker_id = task['assigned_worker']
                if task['status'] == 'running':
                    task['completed_at'] = time.time()
                    task['status'] = 'completed'
                    task['output_data'] = output_data
                    if worker_id and worker_id in self.workers:
                        self.workers[worker_id]['tasks_completed'] += 1
                else: 
                    # if task was not running, we can thrown an error so that it restarts, but we can enforce that by still marking it as failed
                    task['completed_at'] = time.time()
                    task['status'] = 'failed'
                    if worker_id and worker_id in self.workers:
                        self.workers[worker_id]['tasks_failed'] += 1

    def fail_task(self, task_id):
        """Marks a task as failed and reset for retry."""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                worker_id = task['assigned_worker']

                # Update worker stats
                if worker_id and worker_id in self.workers:
                    self.workers[worker_id]['tasks_failed'] += 1

                # Reset for potential retry
                if task['status'] == 'running' and task['attempts'] < 3:  # max 3 attempts
                    task['status'] = 'pending'
                    task['assigned_worker'] = None

    def get_task(self, task_id):
        """Retrieves information about a specific task."""
        with self.lock:
            return self.tasks.get(task_id, {}).copy()

    def get_all_tasks(self):
        """Retrieves information about all tasks."""
        with self.lock:
            return self.tasks.copy()

    def get_pending_tasks(self):
        """Retrieves all pending tasks."""
        with self.lock:
            return [
                (tid, task) for tid, task in self.tasks.items() if task['status'] == 'pending'
            ]

    def reassign_worker_tasks(self, worker_id):
        """Reassigns tasks from a failed worker."""
        with self.lock:
            reassigned_tasks = []
            #print("This function was called")

            for task_id, task in self.tasks.items():
                #if task['assigned_worker'] == worker_id and task['status'] == 'running':
                if task['assigned_worker'] == worker_id:
                    task['status'] = 'pending'
                    task['assigned_worker'] = None  
                    reassigned_tasks.append(task_id)
                    
                    self.workers[worker_id]['tasks_assigned'] -= 1
            if reassigned_tasks:
                print(f"Reassigned tasks {reassigned_tasks} from failed worker {worker_id}")

            return reassigned_tasks
