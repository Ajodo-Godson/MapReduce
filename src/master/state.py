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
                