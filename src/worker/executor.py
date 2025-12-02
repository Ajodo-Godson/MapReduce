import json
import os
from collections import defaultdict

# Import framework components
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.framework.mapper import MapPhase, word_count_map
from src.framework.reducer import ReducePhase, word_count_reduce
from src.framework.shuffler import ShufflePhase
from src.utils.partitioner import Partitioner
from src.worker.intermediate import IntermediateFileManager


class TaskExecutor:
    """Executes map and reduce tasks using the MapReduce framework.
    
    Based on Google MapReduce paper:
    - Map tasks: Apply map function, partition output into R intermediate files
    - Reduce tasks: Fetch intermediate data, shuffle/sort, apply reduce function
    """
    
    def __init__(self, worker_id, num_reduce_tasks=3, 
                 map_function=None, reduce_function=None,
                 intermediate_dir='./intermediate'):
        """Initialize executor with framework components.
        
        Args:
            worker_id: ID of the worker running this executor
            num_reduce_tasks: Number of reduce partitions (R)
            map_function: User-defined map function (default: word_count_map)
            reduce_function: User-defined reduce function (default: word_count_reduce)
            intermediate_dir: Directory for intermediate files
        """
        self.worker_id = worker_id
        self.num_reduce_tasks = num_reduce_tasks
        
        # Use provided functions or defaults (word count)
        self.map_function = map_function or word_count_map
        self.reduce_function = reduce_function or word_count_reduce
        
        # Initialize framework components
        self.partitioner = Partitioner(num_reduce_tasks)
        self.map_phase = MapPhase(self.map_function, self.partitioner)
        self.reduce_phase = ReducePhase(self.reduce_function)
        self.shuffle_phase = ShufflePhase()
        
        # Intermediate file manager
        self.intermediate_manager = IntermediateFileManager(
            worker_id, 
            base_dir=intermediate_dir
        )
    
    def update_num_reduce_tasks(self, num_reduce_tasks):
        """Update R value (called when master sends task assignment)."""
        if num_reduce_tasks != self.num_reduce_tasks:
            self.num_reduce_tasks = num_reduce_tasks
            self.partitioner = Partitioner(num_reduce_tasks)
            self.map_phase = MapPhase(self.map_function, self.partitioner)
    
    def execute_map(self, task_id, input_data, input_key=None):
        """Execute a map task.
        
        1. Apply user's map function to input
        2. Partition output into R buckets using hash(key) mod R
        3. Write partitioned data to local intermediate files
        4. Return file locations to report to master
        
        Args:
            task_id: ID of the map task
            input_data: Input value (e.g., file contents or text chunk)
            input_key: Input key (e.g., filename), defaults to task_id
        
        Returns:
            dict: {partition_id: file_path} mapping for intermediate files
        """
        input_key = input_key or task_id
        
        # Execute map phase: apply map function and partition results
        partitions = self.map_phase.execute(input_key, input_data)
        
        # Write partitioned output to local disk
        file_paths = self.intermediate_manager.write_partitioned_output(
            task_id, 
            partitions
        )
        
        return file_paths
    
    def execute_reduce(self, task_id, partition_id, intermediate_locations):
        """Execute a reduce task.
        
        1. Fetch intermediate files from all map workers for this partition
        2. Group values by key (shuffle)
        3. Sort by key
        4. Apply reduce function to each (key, [values]) group
        5. Return final output
        
        Args:
            task_id: ID of the reduce task
            partition_id: Which partition this reduce handles
            intermediate_locations: List of dicts with file locations
                                   [{'file_path': '...', 'worker_id': '...'}, ...]
        
        Returns:
            dict: {key: reduced_value} final results
        """
        # Parse locations if passed as JSON string
        if isinstance(intermediate_locations, str):
            intermediate_locations = json.loads(intermediate_locations)
        
        # Shuffle phase: fetch and group intermediate data by key
        grouped_data = self._fetch_and_group_intermediate(intermediate_locations)
        
        # Sort by key
        sorted_data = self.shuffle_phase.sort_by_key(grouped_data)
        
        # Reduce phase: apply reduce function to each key's values
        results = self.reduce_phase.execute(sorted_data)
        
        return results
    
    def _fetch_and_group_intermediate(self, intermediate_locations):
        """Fetch intermediate files and group values by key.
        
        This is the shuffle phase from the paper.
        
        Args:
            intermediate_locations: List of file location dicts
        
        Returns:
            dict: {key: [value1, value2, ...]}
        """
        grouped_data = defaultdict(list)
        
        for location in intermediate_locations:
            file_path = location.get('file_path')
            if not file_path or not os.path.exists(file_path):
                print(f"Warning: Intermediate file not found: {file_path}")
                continue
            
            try:
                partition_data = self.intermediate_manager.read_intermediate_file(file_path)
                
                # Group values by key
                for key, values in partition_data.items():
                    if isinstance(values, list):
                        grouped_data[key].extend(values)
                    else:
                        grouped_data[key].append(values)
            except Exception as e:
                print(f"Error reading intermediate file {file_path}: {e}")
        
        return dict(grouped_data)
    
    def cleanup(self, task_id=None):
        """Clean up intermediate files.
        
        Args:
            task_id: If provided, clean up only files for this task.
                    Otherwise, clean up all files.
        """
        if task_id:
            self.intermediate_manager.cleanup_task_files(task_id)
        else:
            # Clean up all intermediate files for this worker
            intermediate_dir = self.intermediate_manager.base_dir
            if os.path.exists(intermediate_dir):
                for f in os.listdir(intermediate_dir):
                    os.remove(os.path.join(intermediate_dir, f))


# Legacy interface for backward compatibility
class SimpleTaksExecutor:
    """Simple executor for testing (no intermediate files)."""
    
    def execute_map(self, input_data):
        """Simple word count mapper (legacy interface)."""
        words = input_data.lower().split()
        word_counts = defaultdict(int)
        
        for word in words:
            word = ''.join(c for c in word if c.isalnum())
            if word:
                word_counts[word] += 1
        
        return dict(word_counts)
    
    def execute_reduce(self, input_data):
        """Simple reducer (legacy interface)."""
        if isinstance(input_data, str):
            data = json.loads(input_data)
        else:
            data = input_data
        return data