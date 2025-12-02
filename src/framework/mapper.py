from collections import defaultdict


class MapPhase:
    """Handles the map phase of MapReduce"""
    
    def __init__(self, map_function, partitioner):
        """
        Args:
            map_function: User-defined map function(key, value) -> [(k', v'), ...]
            partitioner: Partitioner instance for intermediate keys
        """
        self.map_function = map_function
        self.partitioner = partitioner
    
    def execute(self, input_key, input_value):
        """Execute map function and partition results
        
        Args:
            input_key: Input key (e.g., filename)
            input_value: Input value (e.g., file contents)
        
        Returns:
            List of dicts, one per partition: [{k': v', ...}, ...]
        """
        # Execute user's map function
        intermediate_pairs = self.map_function(input_key, input_value)
        
        # Partition the intermediate key-value pairs
        partitions = [defaultdict(list) for _ in range(self.partitioner.num_partitions)]
        
        for key, value in intermediate_pairs:
            partition_id = self.partitioner.get_partition(key)
            partitions[partition_id][key].append(value)
        
        # Convert defaultdicts to regular dicts
        return [dict(p) for p in partitions]


# Example map function for word count
def word_count_map(filename, contents):
    """Map function for word count
    
    Args:
        filename: Name of the file
        contents: Contents of the file
    
    Yields:
        (word, 1) pairs
    """
    words = contents.lower().split()
    for word in words:
        # Clean word
        word = ''.join(c for c in word if c.isalnum())
        if word:
            yield (word, 1)