import json
from collections import defaultdict


class TaskExecutor:
    """Executes map and reduce tasks"""
    
    def execute_map(self, input_data):
        """
        Simple word count mapper
        Input: A line of text
        Output: Dictionary of word -> count
        """
        words = input_data.lower().split()
        word_counts = defaultdict(int)
        
        for word in words:
            # Clean word (remove punctuation)
            word = ''.join(c for c in word if c.isalnum())
            if word:
                word_counts[word] = word_counts[word] + 1
        
        return dict(word_counts)
    
    def execute_reduce(self, input_data):
        """
        Simple word count reducer
        Input: JSON string of {word: count} pairs
        Output: Aggregated counts
        """
        data = json.loads(input_data)
        
        # For now, just pass through
        # In a real implementation, this would aggregate multiple map outputs
        return data