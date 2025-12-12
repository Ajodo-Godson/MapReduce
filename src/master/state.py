import threading
import time
from datetime import datetime


class MasterState:
    """Manages the state of workers and tasks with idempotency support.
    
    Duplicate Job Handling:
    -----------------------
    The scenario: Worker A takes task, goes slow, misses heartbeat, gets marked failed.
    Task is reassigned to Worker B. Now both A and B might complete the same task.
    
    Solution: Track task completion by task_id (not just sequence_number).
    - When a task is already 'completed', ignore subsequent completions
    - First completion wins, later completions are logged but ignored
    - This is safe because map/reduce operations are idempotent by design:
      * Same input always produces same output
      * Writing same intermediate file twice = same result
      * Reduce output is deterministic
    """
    
    def __init__(self):
        self.workers = {}  # worker_id -> worker_info
        self.tasks = {}    # task_id -> task_info
        self.task_sequence = 0  # Monotonically increasing sequence number
        self.completed_task_ids = set()  # Track completed tasks for idempotency
        self.intermediate_files = {}  # task_id -> {partition_id -> file_location}
        self.task_history = []  # For visualization: list of (timestamp, event, details)
        self.lock = threading.Lock()
    
    def _log_event(self, event_type, details):
        """Log an event for visualization."""
        self.task_history.append({
            'timestamp': time.time(),
            'event': event_type,
            'details': details
        })
        # Keep only last 100 events
        if len(self.task_history) > 100:
            self.task_history = self.task_history[-100:]
    
    def add_worker(self, worker_id, worker_info):
        """Register a new worker"""
        with self.lock:
            self.workers[worker_id] = {
                **worker_info,
                'registered_at': time.time(),
                'last_heartbeat': time.time(),
                'tasks_completed': 0,
                'tasks_failed': 0,
                'current_tasks': [],
                'task_history': []  # Track what tasks this worker has done
            }
            self._log_event('worker_registered', {'worker_id': worker_id})
    
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
                self._log_event('worker_failed', {'worker_id': worker_id})
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
                    'last_heartbeat_raw': info['last_heartbeat'],
                    'tasks_completed': info.get('tasks_completed', 0),
                    'tasks_failed': info.get('tasks_failed', 0),
                    'current_tasks': info.get('current_tasks', []),
                    'registered_at': info.get('registered_at', 0)
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
                'sequence_number': self.task_sequence,
                'completion_worker': None,  # Who actually completed it (for duplicate tracking)
                'duplicate_completions': []  # Track any duplicate completions
            }
            self._log_event('task_created', {'task_id': task_id, 'task_type': task_info.get('task_type')})
    
    def assign_task(self, task_id, worker_id):
        """Assign a task to a worker"""
        with self.lock:
            if task_id in self.tasks:
                # Don't reassign if already completed
                if self.tasks[task_id]['status'] == 'completed':
                    return False
                
                self.tasks[task_id]['status'] = 'running'
                self.tasks[task_id]['assigned_worker'] = worker_id
                self.tasks[task_id]['started_at'] = time.time()
                self.tasks[task_id]['attempts'] += 1
                
                # Add to worker's current tasks
                if worker_id in self.workers:
                    self.workers[worker_id]['current_tasks'].append(task_id)
                
                self._log_event('task_assigned', {
                    'task_id': task_id, 
                    'worker_id': worker_id,
                    'attempt': self.tasks[task_id]['attempts']
                })
                return True
        return False
    
    def complete_task(self, task_id, sequence_number, output_data, worker_id=None):
        """Mark task as completed (idempotent).
        
        Handles the duplicate completion scenario:
        - If task already completed, log as duplicate but don't error
        - First completion wins
        - Duplicate completions are recorded for debugging/monitoring
        
        Returns: True if this was a duplicate completion, False otherwise
        """
        with self.lock:
            # Check if task already completed (primary idempotency check)
            if task_id in self.completed_task_ids:
                # This is a duplicate completion - log it but don't fail
                if task_id in self.tasks:
                    self.tasks[task_id]['duplicate_completions'].append({
                        'worker_id': worker_id,
                        'timestamp': time.time(),
                        'sequence_number': sequence_number
                    })
                self._log_event('duplicate_completion', {
                    'task_id': task_id,
                    'worker_id': worker_id,
                    'original_worker': self.tasks.get(task_id, {}).get('completion_worker')
                })
                print(f"[{datetime.now()}] ⚠️  DUPLICATE completion for task {task_id} "
                      f"from worker {worker_id} (already completed by "
                      f"{self.tasks.get(task_id, {}).get('completion_worker')})")
                return True  # Is duplicate
            
            if task_id in self.tasks:
                self.tasks[task_id]['status'] = 'completed'
                self.tasks[task_id]['completed_at'] = time.time()
                self.tasks[task_id]['output_data'] = output_data
                self.tasks[task_id]['completion_worker'] = worker_id or self.tasks[task_id]['assigned_worker']
                
                # Mark task as completed (idempotency tracking)
                self.completed_task_ids.add(task_id)
                
                # Update worker stats - credit the worker who actually completed it
                completing_worker = worker_id or self.tasks[task_id]['assigned_worker']
                if completing_worker and completing_worker in self.workers:
                    self.workers[completing_worker]['tasks_completed'] += 1
                    self.workers[completing_worker]['task_history'].append({
                        'task_id': task_id,
                        'action': 'completed',
                        'timestamp': time.time()
                    })
                    # Remove from current tasks
                    if task_id in self.workers[completing_worker]['current_tasks']:
                        self.workers[completing_worker]['current_tasks'].remove(task_id)
                
                self._log_event('task_completed', {
                    'task_id': task_id,
                    'worker_id': completing_worker,
                    'task_type': self.tasks[task_id].get('task_type')
                })
                
                return False  # Not a duplicate
    
    def fail_task(self, task_id):
        """Mark task as failed and reset for retry"""
        with self.lock:
            if task_id in self.tasks:
                worker_id = self.tasks[task_id]['assigned_worker']
                
                # Update worker stats
                if worker_id and worker_id in self.workers:
                    self.workers[worker_id]['tasks_failed'] += 1
                    # Remove from current tasks
                    if task_id in self.workers[worker_id]['current_tasks']:
                        self.workers[worker_id]['current_tasks'].remove(task_id)
                
                # Reset for potential retry
                if self.tasks[task_id]['attempts'] < 3:  # Max 3 attempts
                    self.tasks[task_id]['status'] = 'pending'
                    self.tasks[task_id]['assigned_worker'] = None
                    self._log_event('task_retry', {
                        'task_id': task_id,
                        'attempt': self.tasks[task_id]['attempts']
                    })
                else:
                    self.tasks[task_id]['status'] = 'failed'
                    self._log_event('task_failed_permanently', {
                        'task_id': task_id,
                        'attempts': self.tasks[task_id]['attempts']
                    })
    
    def get_task(self, task_id):
        """Get task info"""
        with self.lock:
            return self.tasks.get(task_id, {}).copy()
    
    def get_all_tasks(self):
        """Get all tasks"""
        with self.lock:
            return self.tasks.copy()
    
    def get_recent_events(self, count=20):
        """Get recent events for dashboard visualization."""
        with self.lock:
            return self.task_history[-count:] if self.task_history else []
    
    def get_pending_tasks(self):
        """Get all pending tasks"""
        with self.lock:
            return [
                (tid, task) 
                for tid, task in self.tasks.items() 
                if task['status'] == 'pending'
            ]
    
    def reassign_worker_tasks(self, worker_id):
        """Reassign tasks from a failed worker.
        
        Note: We only reassign RUNNING tasks, not completed ones.
        If the "failed" worker actually completes its task later,
        the duplicate completion will be detected and ignored.
        """
        with self.lock:
            reassigned = []
            for task_id, task in self.tasks.items():
                # Only reassign running tasks (not already completed)
                if (task['assigned_worker'] == worker_id and 
                    task['status'] == 'running' and
                    task_id not in self.completed_task_ids):
                    task['status'] = 'pending'
                    task['assigned_worker'] = None
                    reassigned.append(task_id)
                    self._log_event('task_reassigned', {
                        'task_id': task_id,
                        'from_worker': worker_id,
                        'reason': 'worker_failed'
                    })
            
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
    
    # ==================== VISUALIZATION METHODS ====================
    
    def get_system_visualization(self):
        """Get complete system state for visualization.
        
        Returns a structured view of:
        - Worker states and their current work
        - Task states grouped by phase (map/reduce)
        - Recent events timeline
        - Job progress metrics
        """
        with self.lock:
            current_time = time.time()
            
            # Worker visualization
            workers_viz = {}
            for wid, info in self.workers.items():
                time_since_hb = current_time - info['last_heartbeat']
                workers_viz[wid] = {
                    'status': info['status'],
                    'status_icon': self._get_status_icon(info['status']),
                    'current_tasks': info.get('current_tasks', []),
                    'tasks_completed': info.get('tasks_completed', 0),
                    'tasks_failed': info.get('tasks_failed', 0),
                    'last_heartbeat_ago': f"{time_since_hb:.1f}s ago",
                    'healthy': time_since_hb < 10 and info['status'] != 'failed'
                }
            
            # Task visualization by phase
            map_tasks = {'pending': [], 'running': [], 'completed': [], 'failed': []}
            reduce_tasks = {'pending': [], 'running': [], 'completed': [], 'failed': []}
            
            for tid, task in self.tasks.items():
                task_viz = {
                    'task_id': tid,
                    'worker': task.get('assigned_worker') or task.get('completion_worker'),
                    'attempts': task.get('attempts', 0),
                    'has_duplicates': len(task.get('duplicate_completions', [])) > 0
                }
                
                if task['task_type'] == 'map':
                    map_tasks[task['status']].append(task_viz)
                else:
                    reduce_tasks[task['status']].append(task_viz)
            
            # Calculate progress
            total_map = sum(len(v) for v in map_tasks.values())
            total_reduce = sum(len(v) for v in reduce_tasks.values())
            
            return {
                'workers': workers_viz,
                'map_tasks': map_tasks,
                'reduce_tasks': reduce_tasks,
                'progress': {
                    'map_progress': len(map_tasks['completed']) / total_map * 100 if total_map > 0 else 0,
                    'reduce_progress': len(reduce_tasks['completed']) / total_reduce * 100 if total_reduce > 0 else 0,
                    'overall': (len(map_tasks['completed']) + len(reduce_tasks['completed'])) / 
                              (total_map + total_reduce) * 100 if (total_map + total_reduce) > 0 else 0
                },
                'recent_events': self.task_history[-10:],  # Last 10 events
                'duplicate_count': sum(
                    len(t.get('duplicate_completions', [])) 
                    for t in self.tasks.values()
                )
            }
    
    def _get_status_icon(self, status):
        """Get emoji icon for status."""
        icons = {
            'idle': '😴',
            'busy': '⚙️',
            'failed': '💀',
            'pending': '⏳',
            'running': '🔄',
            'completed': '✅'
        }
        return icons.get(status, '❓')
    
    def get_ascii_visualization(self):
        """Generate ASCII art visualization of system state."""
        viz = self.get_system_visualization()
        lines = []
        
        # Header
        lines.append("╔" + "═" * 70 + "╗")
        lines.append("║" + "MAPREDUCE SYSTEM STATE".center(70) + "║")
        lines.append("╠" + "═" * 70 + "╣")
        
        # Workers section
        lines.append("║ WORKERS:".ljust(71) + "║")
        for wid, info in viz['workers'].items():
            health = "🟢" if info['healthy'] else "🔴"
            status = info['status']
            
            # Only show task info if worker is active (not failed)
            if status == 'failed':
                line = f"  {health} {wid}: {info['status_icon']} {status}"
            elif info['current_tasks']:
                task_str = f"[{', '.join(info['current_tasks'])}]"
                line = f"  {health} {wid}: {info['status_icon']} {status} {task_str}"
            else:
                line = f"  {health} {wid}: {info['status_icon']} {status}"
            lines.append("║" + line.ljust(70) + "║")
        
        lines.append("╠" + "═" * 70 + "╣")
        
        # Map phase
        map_t = viz['map_tasks']
        lines.append("║ MAP PHASE:".ljust(71) + "║")
        progress_bar = self._make_progress_bar(viz['progress']['map_progress'], 40)
        lines.append("║" + f"  {progress_bar} {viz['progress']['map_progress']:.0f}%".ljust(70) + "║")
        lines.append("║" + f"  ⏳ Pending: {len(map_t['pending'])} | 🔄 Running: {len(map_t['running'])} | ✅ Done: {len(map_t['completed'])}".ljust(70) + "║")
        
        lines.append("╠" + "═" * 70 + "╣")
        
        # Reduce phase
        red_t = viz['reduce_tasks']
        lines.append("║ REDUCE PHASE:".ljust(71) + "║")
        progress_bar = self._make_progress_bar(viz['progress']['reduce_progress'], 40)
        lines.append("║" + f"  {progress_bar} {viz['progress']['reduce_progress']:.0f}%".ljust(70) + "║")
        lines.append("║" + f"  ⏳ Pending: {len(red_t['pending'])} | 🔄 Running: {len(red_t['running'])} | ✅ Done: {len(red_t['completed'])}".ljust(70) + "║")
        
        # Duplicate warnings
        if viz['duplicate_count'] > 0:
            lines.append("╠" + "═" * 70 + "╣")
            lines.append("║" + f" ⚠️  Duplicate completions detected: {viz['duplicate_count']}".ljust(70) + "║")
        
        # Recent events
        lines.append("╠" + "═" * 70 + "╣")
        lines.append("║ RECENT EVENTS:".ljust(71) + "║")
        for event in viz['recent_events'][-5:]:
            ts = time.strftime('%H:%M:%S', time.localtime(event['timestamp']))
            evt_line = f"  [{ts}] {event['event']}: {event['details']}"
            if len(evt_line) > 68:
                evt_line = evt_line[:65] + "..."
            lines.append("║" + evt_line.ljust(70) + "║")
        
        lines.append("╚" + "═" * 70 + "╝")
        
        return "\n".join(lines)
    
    def _make_progress_bar(self, percentage, width):
        """Create ASCII progress bar."""
        filled = int(width * percentage / 100)
        empty = width - filled
        return f"[{'█' * filled}{'░' * empty}]"
    
    def reset_for_new_job(self):
        """Reset state for a new job (used by DAG scheduler)."""
        with self.lock:
            self.tasks = {}
            self.task_sequence = 0
            self.completed_task_ids = set()
            self.intermediate_files = {}
            self.task_history = []
            # Don't reset workers - they stay connected
            for wid in self.workers:
                self.workers[wid]['current_tasks'] = []
            self._log_event('job_reset', {'message': 'State reset for new job'})
    
    def get_reduce_outputs(self):
        """Get all reduce task outputs combined."""
        with self.lock:
            combined_output = {}
            for task_id, task_info in self.tasks.items():
                if task_info.get('task_type') == 'reduce' and task_info.get('status') == 'completed':
                    output_data = task_info.get('output_data')
                    if output_data:
                        try:
                            import json
                            if isinstance(output_data, str):
                                data = json.loads(output_data)
                            else:
                                data = output_data
                            if isinstance(data, dict):
                                combined_output.update(data)
                        except (json.JSONDecodeError, TypeError):
                            pass
            return combined_output