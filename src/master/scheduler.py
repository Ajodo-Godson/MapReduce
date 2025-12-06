import json
from datetime import datetime


class TaskScheduler:
    """Handles task creation and assignment with map→reduce phase coordination.
    
    Based on Google MapReduce paper:
    - All M map tasks must complete before reduce phase begins
    - R reduce tasks are created after map phase completion
    - Each reduce task processes one partition of intermediate data
    """
    
    def __init__(self, state, num_reduce_tasks=3):
        self.state = state
        self.num_reduce_tasks = num_reduce_tasks
        self.task_counter = 0
        self.map_phase_complete = False
        self.reduce_phase_started = False
        self.total_map_tasks = 0
    
    def create_map_tasks(self, input_data):
        """Create map tasks from input data (M splits).
        
        Each input split becomes one map task.
        Map tasks will partition their output into R intermediate files.
        
        Args:
            input_data: List of input splits (strings or file paths)
        """
        for i, data in enumerate(input_data):
            task_id = f"map_{i}"
            task_info = {
                'task_type': 'map',
                'task_id': task_id,
                'input_data': data,
                'partition_id': i,  # Which split this map processes
                'num_reduce_tasks': self.num_reduce_tasks  # R value for partitioning
            }
            self.state.add_task(task_id, task_info)
            self.task_counter += 1
            self.total_map_tasks += 1
        
        print(f"[{datetime.now()}] Created {self.total_map_tasks} map tasks, "
              f"will create {self.num_reduce_tasks} reduce tasks after map phase")
    
    def create_reduce_tasks(self):
        """Create reduce tasks after all map tasks complete.
        
        Each reduce task:
        - Processes partition i (0 to R-1)
        - Fetches intermediate files from all mappers for partition i
        - Applies reduce function to grouped key-value pairs
        """
        if self.reduce_phase_started:
            return  # Already created reduce tasks
        
        print(f"[{datetime.now()}] Map phase complete! Creating {self.num_reduce_tasks} reduce tasks...")
        
        for partition_id in range(self.num_reduce_tasks):
            task_id = f"reduce_{partition_id}"
            
            # Get intermediate file locations for this partition from all completed map tasks
            intermediate_locations = self.state.get_intermediate_files_for_partition(partition_id)
            
            task_info = {
                'task_type': 'reduce',
                'task_id': task_id,
                'partition_id': partition_id,
                'input_data': json.dumps(intermediate_locations),  # Locations to fetch from
                'num_reduce_tasks': self.num_reduce_tasks
            }
            self.state.add_task(task_id, task_info)
            self.task_counter += 1
        
        self.reduce_phase_started = True
        print(f"[{datetime.now()}] Created {self.num_reduce_tasks} reduce tasks")
    
    def check_map_phase_complete(self):
        """Check if all map tasks are completed and transition to reduce phase."""
        if self.map_phase_complete:
            return True
        
        all_tasks = self.state.get_all_tasks()
        map_tasks = [t for t in all_tasks.values() if t['task_type'] == 'map']
        
        if not map_tasks:
            return False
        
        completed_map_tasks = sum(1 for t in map_tasks if t['status'] == 'completed')
        
        if completed_map_tasks == len(map_tasks):
            self.map_phase_complete = True
            print(f"[{datetime.now()}] ✅ All {len(map_tasks)} map tasks completed!")
            # Trigger reduce task creation
            self.create_reduce_tasks()
            return True
        
        return False
    
    def check_job_complete(self):
        """Check if entire MapReduce job is complete."""
        all_tasks = self.state.get_all_tasks()
        
        if not all_tasks:
            return False
        
        return all(t['status'] == 'completed' for t in all_tasks.values())
    
    def get_next_task(self, worker_id):
        """Get the next pending task for a worker.
        
        Scheduling policy:
        1. During map phase: assign pending map tasks (FIFO)
        2. After map phase: check for completion, create reduce tasks
        3. During reduce phase: assign pending reduce tasks (FIFO)
        """
        # Check if we should transition to reduce phase
        self.check_map_phase_complete()
        
        pending_tasks = self.state.get_pending_tasks()
        
        if not pending_tasks:
            # Check if job is complete
            if self.check_job_complete():
                print(f"[{datetime.now()}] 🎉 MapReduce job complete!")
            return None
        
        # Prioritize map tasks over reduce tasks (shouldn't matter due to phase ordering)
        # but ensures map tasks complete before reduce if any stragglers
        map_tasks = [(tid, t) for tid, t in pending_tasks if t['task_type'] == 'map']
        reduce_tasks = [(tid, t) for tid, t in pending_tasks if t['task_type'] == 'reduce']
        
        # Assign map tasks first, then reduce tasks
        if map_tasks:
            task_id, task_info = map_tasks[0]
        elif reduce_tasks:
            task_id, task_info = reduce_tasks[0]
        else:
            return None
        
        # Assign task to worker
        self.state.assign_task(task_id, worker_id)
        
        print(f"[{datetime.now()}] Assigned {task_info['task_type']} task {task_id} to worker {worker_id}")
        
        return {
            'task_id': task_id,
            'task_type': task_info['task_type'],
            'input_data': task_info['input_data'],
            'partition_id': task_info['partition_id'],
            'num_reduce_tasks': task_info.get('num_reduce_tasks', self.num_reduce_tasks),
            'sequence_number': task_info.get('sequence_number', 0)
        }
    
    def mark_task_complete(self, task_id, worker_id, output_data, sequence_number=None):
        """Mark a task as completed.
        
        Handles duplicate completions gracefully:
        - If worker A was slow and got "failed", task reassigned to B
        - If A actually completes later, it's detected as duplicate
        - First completion wins, duplicate is logged but ignored
        
        Args:
            task_id: ID of completed task
            worker_id: Worker that completed the task (important for duplicate detection)
            output_data: Task output (for reduce) or intermediate file locations (for map)
            sequence_number: For idempotency checking
        """
        task = self.state.get_task(task_id)
        seq = sequence_number or task.get('sequence_number', 0)
        
        # Pass worker_id to complete_task for proper tracking
        is_duplicate = self.state.complete_task(task_id, seq, output_data, worker_id=worker_id)
        
        if not is_duplicate and task.get('task_type') == 'map':
            # After map task completes, check if we can start reduce phase
            self.check_map_phase_complete()
        
        return is_duplicate
    
    def mark_task_failed(self, task_id, worker_id):
        """Mark a task as failed (will be retried)."""
        self.state.fail_task(task_id)
    
    def get_job_progress(self):
        """Get current job progress statistics."""
        all_tasks = self.state.get_all_tasks()
        
        map_tasks = [t for t in all_tasks.values() if t['task_type'] == 'map']
        reduce_tasks = [t for t in all_tasks.values() if t['task_type'] == 'reduce']
        
        return {
            'map_total': len(map_tasks),
            'map_completed': sum(1 for t in map_tasks if t['status'] == 'completed'),
            'map_running': sum(1 for t in map_tasks if t['status'] == 'running'),
            'map_pending': sum(1 for t in map_tasks if t['status'] == 'pending'),
            'reduce_total': len(reduce_tasks),
            'reduce_completed': sum(1 for t in reduce_tasks if t['status'] == 'completed'),
            'reduce_running': sum(1 for t in reduce_tasks if t['status'] == 'running'),
            'reduce_pending': sum(1 for t in reduce_tasks if t['status'] == 'pending'),
            'map_phase_complete': self.map_phase_complete,
            'job_complete': self.check_job_complete()
        }