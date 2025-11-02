"""
A simplified MapReduce framework
"""
import os
import json
from typing import List, Tuple, Any, Callable, Dict


class SimpleMapReduce:
    def __init__(self, num_mappers: int = 1, num_reducers: int = 1):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def map_reduce(self, 
                   data: List[Any], 
                   map_func: Callable, 
                   reduce_func: Callable) -> List[Any]:
        """
        Execute the MapReduce job on the given data
        """
        # Partition data for mappers
        partitions = self._partition_data(data)
        
        # Apply map function to each partition
        mapped_results = []
        for partition in partitions:
            mapped_partition = []
            for item in partition:
                mapped_items = map_func(item)
                mapped_partition.extend(mapped_items)
            mapped_results.append(mapped_partition)
        
        # Group by key for reducers
        grouped_data = self._group_by_key(mapped_results)
        
        # Apply reduce function to each group
        final_results = []
        for key, values in grouped_data.items():
            reduced_result = reduce_func(key, values)
            final_results.append(reduced_result)
        
        return final_results

    def _partition_data(self, data: List[Any]) -> List[List[Any]]:
        """Partition input data among mappers"""
        partitions = [[] for _ in range(self.num_mappers)]
        for i, item in enumerate(data):
            partitions[i % self.num_mappers].append(item)
        return partitions

    def _group_by_key(self, mapped_results: List[List[Tuple[Any, Any]]]) -> Dict[Any, List[Any]]:
        """Group mapped results by key for reducers"""
        grouped = {}
        for partition in mapped_results:
            for key, value in partition:
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(value)
        return grouped

    def map_only(self, data: List[Any], map_func: Callable) -> List[Any]:
        """
        Execute only the Map phase of the MapReduce job
        """
        # Partition data for mappers
        partitions = self._partition_data(data)
        
        # Apply map function to each partition
        mapped_results = []
        for partition in partitions:
            mapped_partition = []
            for item in partition:
                mapped_items = map_func(item)
                if mapped_items:
                    mapped_partition.extend(mapped_items)
            if mapped_partition:
                mapped_results.extend(mapped_partition)
        
        return mapped_results

    def __call__(self, data: List[Any], map_func: Callable, reduce_func: Callable) -> List[Any]:
        """Allow calling the class instance as a function"""
        return self.map_reduce(data, map_func, reduce_func)