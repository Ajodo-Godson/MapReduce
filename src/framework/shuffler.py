import json
from collections import defaultdict


class ShufflePhase:
    """Handles the shuffle phase - fetching and grouping intermediate data"""
    
    def fetch_and_group(self, intermediate_locations):
        """Fetch intermediate files and group by key
        
        Args:
            intermediate_locations: List of {worker_id, file_path} dicts
        
        Returns:
            Dict of {key: [value1, value2, ...]}
        """
        grouped_data = defaultdict(list)
        
        for location in intermediate_locations:
            # Read intermediate file
            with open(location['file_path'], 'r') as f:
                partition_data = json.load(f)
            
            # Group values by key
            for key, values in partition_data.items():
                if isinstance(values, list):
                    grouped_data[key].extend(values)
                else:
                    grouped_data[key].append(values)
        
        return dict(grouped_data)
    
    def sort_by_key(self, grouped_data):
        """Sort grouped data by key
        
        Args:
            grouped_data: Dict of {key: [values]}
        
        Returns:
            List of (key, [values]) tuples sorted by key
        """
        return sorted(grouped_data.items())