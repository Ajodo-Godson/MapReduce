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
                'tasks_failed': 0
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
                # Reset tasks assigned to this worker including the completed ones
                for task_id, task in self.tasks.items():
                    if task['assigned_worker'] == worker_id:
                        task['status'] = 'idle'
                        task['assigned_worker'] = None
                
    def get_worker(self, worker_id):
        """Retrieves information about a specific worker."""

        pass
       
    def get_all_workers(self):
        """Retrieves information about all workers."""
        pass

    def add_task(self, task_id, task_info):
        """Adds a new task to the master."""
        pass

    def assign_task(self, worker_id):
        """Assigns an idle task to a worker."""
        pass

    def complete_task(self, worker_id, task_id, success):
        """Marks a task as complete or failed."""
        pass

    def fail_task(self, task_id):
        """Marks a task as failed."""
        pass

    def get_task(self, task_id):
        """Retrieves information about a specific task."""
        pass

    def get_all_tasks(self):
        """Retrieves information about all tasks."""
        pass

    def get_pending_tasks(self):
        """Retrieves all pending tasks."""
        pass

    def reassign_worker_tasks(self, worker_id):
        """Reassigns all tasks from a failed worker."""
        pass
    