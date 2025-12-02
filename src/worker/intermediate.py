import json
import os
import tempfile


class IntermediateFileManager:
    """Manages intermediate files for map outputs"""
    
    def __init__(self, worker_id, base_dir='./intermediate'):
        self.worker_id = worker_id
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
    
    def write_partitioned_output(self, task_id, partitions):
        """Write partitioned map output to local disk
        
        Args:
            task_id: ID of the map task
            partitions: List of dicts, one per partition
        
        Returns:
            Dict of {partition_id: file_path}
        """
        file_paths = {}
        
        for partition_id, data in enumerate(partitions):
            filename = f"{task_id}_partition_{partition_id}.json"
            filepath = os.path.join(self.base_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(data, f)
            
            file_paths[partition_id] = filepath
        
        return file_paths
    
    def read_intermediate_file(self, filepath):
        """Read an intermediate file"""
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def cleanup_task_files(self, task_id):
        """Clean up intermediate files for a task"""
        pattern = f"{task_id}_partition_"
        for filename in os.listdir(self.base_dir):
            if filename.startswith(pattern):
                os.remove(os.path.join(self.base_dir, filename))