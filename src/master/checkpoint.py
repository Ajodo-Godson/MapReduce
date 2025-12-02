import json
import os
import time
import threading
from datetime import datetime


class MasterCheckpoint:
    """Handles master state checkpointing for fault tolerance"""
    
    def __init__(self, checkpoint_dir='./checkpoints', interval=30):
        self.checkpoint_dir = checkpoint_dir
        self.interval = interval  # Checkpoint every 30 seconds
        self.running = False
        self.checkpoint_thread = None
        
        # Create checkpoint directory
        os.makedirs(checkpoint_dir, exist_ok=True)
    
    def save_checkpoint(self, state):
        """Save current master state to disk"""
        checkpoint_data = {
            'timestamp': time.time(),
            'workers': {wid: self._serialize_worker(w) for wid, w in state.workers.items()},
            'tasks': {tid: self._serialize_task(t) for tid, t in state.tasks.items()},
            'intermediate_files': getattr(state, 'intermediate_files', {})
        }
        
        # Write to temporary file first
        temp_path = os.path.join(self.checkpoint_dir, 'checkpoint.tmp')
        final_path = os.path.join(self.checkpoint_dir, 'checkpoint.json')
        
        with open(temp_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        # Atomic rename
        os.rename(temp_path, final_path)
        
        print(f"[{datetime.now()}] Checkpoint saved")
    
    def load_checkpoint(self):
        """Load the most recent checkpoint"""
        checkpoint_path = os.path.join(self.checkpoint_dir, 'checkpoint.json')
        
        if not os.path.exists(checkpoint_path):
            return None
        
        try:
            with open(checkpoint_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[{datetime.now()}] Failed to load checkpoint: {e}")
            return None
    
    def start_periodic_checkpointing(self, state):
        """Start background thread for periodic checkpointing"""
        self.running = True
        
        def checkpoint_loop():
            while self.running:
                time.sleep(self.interval)
                try:
                    self.save_checkpoint(state)
                except Exception as e:
                    print(f"[{datetime.now()}] Checkpoint failed: {e}")
        
        self.checkpoint_thread = threading.Thread(target=checkpoint_loop, daemon=True)
        self.checkpoint_thread.start()
    
    def stop(self):
        """Stop checkpointing"""
        self.running = False
    
    def _serialize_worker(self, worker):
        """Convert worker dict to serializable format"""
        return {
            'host': worker.get('host'),
            'port': worker.get('port'),
            'status': worker.get('status'),
            'last_heartbeat': worker.get('last_heartbeat'),
            'tasks_completed': worker.get('tasks_completed', 0),
            'tasks_failed': worker.get('tasks_failed', 0)
        }
    
    def _serialize_task(self, task):
        """Convert task dict to serializable format"""
        return {
            'task_type': task.get('task_type'),
            'task_id': task.get('task_id'),
            'input_data': task.get('input_data'),
            'partition_id': task.get('partition_id'),
            'status': task.get('status'),
            'assigned_worker': task.get('assigned_worker'),
            'attempts': task.get('attempts', 0),
            'sequence_number': task.get('sequence_number', 0)
        }