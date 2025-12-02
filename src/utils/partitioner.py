import hashlib


class Partitioner:
    """Hash-based partitioning for intermediate keys"""
    
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def get_partition(self, key):
        """Get partition ID for a key using hash function"""
        # Convert key to string if it isn't already
        key_str = str(key)
        
        # Use hash function (hash(key) mod R)
        hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
        partition_id = hash_value % self.num_partitions
        
        return partition_id
    
    def partition_data(self, data):
        """Partition a dictionary of key-value pairs
        
        Args:
            data: Dict of {key: value}
        
        Returns:
            List of dicts, one per partition
        """
        partitions = [{} for _ in range(self.num_partitions)]
        
        for key, value in data.items():
            partition_id = self.get_partition(key)
            partitions[partition_id][key] = value
        
        return partitions