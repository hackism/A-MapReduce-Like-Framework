"""
Test script specifically for Natural Join functionality
"""
from mapreduce import SimpleMapReduce
from jobs import natural_join_mapper, natural_join_reducer


def demo_natural_join():
    print("=== Natural Join Demo ===")
    
    # Sample data: Table 1 (ID, Name)
    table1_data = [
        (1, ("John", "Engineer")),
        (1, ("Jane", "Manager")),
        (2, ("Bob", "Designer"))
    ]
    
    # Sample data: Table 2 (ID, Department)
    table2_data = [
        (2, ("John", "IT")),
        (2, ("Jane", "HR")),
        (1, ("Alice", "Finance"))
    ]
    
    # Combine both tables
    all_data = table1_data + table2_data
    
    # Create MapReduce instance
    mr = SimpleMapReduce(num_mappers=2, num_reducers=2)
    
    # Run natural join
    results = mr.map_reduce(all_data, natural_join_mapper, natural_join_reducer)
    
    # Print results
    print("Results:")
    for result in results:
        print(f"  {result}")
    print()


if __name__ == "__main__":
    demo_natural_join()