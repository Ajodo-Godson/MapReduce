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

    def add_worker(self, worker_id: str):
        """Registers a new worker with the master."""
        with self.lock:
            self.workers[worker_id] = {
                **worker_info, 
                'registered_at': time.time(),
                'last_heartbeat': time.time()
                'tasks_completed': 0,
                'tasks_failed': 0
            }