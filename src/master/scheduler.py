import json
from datetime import datetime


## PS: I will revisit these. 
## I might consider a toposort design based on time and possible dependencies. 
## Something more rigorous than just the basic FIFO I'm implementing now. 

class TaskScheduler:
    """Handles task creation and assignment"""
    
    def __init__(self, state):
        self.state = state
        self.task_counter = 0
    
    def create_map_tasks(self, input_data):
        """Create map tasks from input data
        
        For this milestone: simple word count
        Each line becomes a map task
        """
        for i, line in enumerate(input_data):
            task_id = f"map_{i}"
            task_info = {
                'task_type': 'map',
                'task_id': task_id,
                'input_data': line,
                'partition_id': i
            }
            self.state.add_task(task_id, task_info)
            self.task_counter += 1
    
    def get_next_task(self, worker_id):
        """Get the next pending task for a worker"""
        pending_tasks = self.state.get_pending_tasks()
        
        if not pending_tasks:
            return None
        
        # Simple FIFO scheduling
        task_id, task_info = pending_tasks[0]
        
        # Assign task to worker
        self.state.assign_task(task_id, worker_id)
        
        print(f"[{datetime.now()}] Assigned task {task_id} to worker {worker_id}")
        
        return {
            'task_id': task_id,
            'task_type': task_info['task_type'],
            'input_data': task_info['input_data'],
            'partition_id': task_info['partition_id']
        }
    
    def mark_task_complete(self, task_id, worker_id, output_data):
        """Mark a task as completed"""
        self.state.complete_task(task_id, output_data)
    
    def mark_task_failed(self, task_id, worker_id):
        """Mark a task as failed"""
        self.state.fail_task(task_id)