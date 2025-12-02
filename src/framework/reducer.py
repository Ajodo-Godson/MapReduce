class ReducePhase:
    """Handles the reduce phase of MapReduce"""
    
    def __init__(self, reduce_function):
        """
        Args:
            reduce_function: User-defined reduce function(key, values) -> result
        """
        self.reduce_function = reduce_function
    
    def execute(self, sorted_data):
        """Execute reduce function on sorted data
        
        Args:
            sorted_data: List of (key, [values]) tuples
        
        Returns:
            Dict of {key: reduced_value}
        """
        results = {}
        
        for key, values in sorted_data:
            result = self.reduce_function(key, values)
            results[key] = result
        
        return results


# Example reduce function for word count
def word_count_reduce(word, counts):
    """Reduce function for word count
    
    Args:
        word: The word
        counts: List of counts (all "1"s)
    
    Returns:
        Total count
    """
    return sum(int(c) for c in counts)