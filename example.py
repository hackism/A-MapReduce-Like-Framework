"""
Example usage of the simple MapReduce framework
"""
from mapreduce import SimpleMapReduce
from jobs import word_count_mapper, word_count_reducer, inverted_index_mapper, inverted_index_reducer


def demo_word_count():
    print("=== Word Count Demo ===")
    
    # Sample data
    text_data = [
        "Hello world this is a test",
        "Hello again world of testing",
        "Testing is important for world building"
    ]
    
    # Create MapReduce instance
    mr = SimpleMapReduce(num_mappers=2, num_reducers=2)
    
    # Run word count job
    results = mr.map_reduce(text_data, word_count_mapper, word_count_reducer)
    
    # Print results
    print("Results:")
    for word, count in sorted(results, key=lambda x: x[1], reverse=True):
        print(f"  {word}: {count}")
    print()


def demo_inverted_index():
    print("=== Inverted Index Demo ===")
    
    # Sample data (filename, content)
    file_data = [
        ("file1.txt", "Hello world this is document one"),
        ("file2.txt", "Hello again world of document two"),
        ("file3.txt", "Document three has different world")
    ]
    
    # Create MapReduce instance
    mr = SimpleMapReduce(num_mappers=2, num_reducers=2)
    
    # Run inverted index job
    results = mr.map_reduce(file_data, inverted_index_mapper, inverted_index_reducer)
    
    # Print results
    print("Results:")
    for word, files in sorted(results):
        print(f"  {word}: {files}")
    print()


def demo_custom_job():
    print("=== Custom Job Demo (Sum Numbers by Category) ===")
    
    # Sample data (category, value)
    data = [
        ("A", 10),
        ("B", 20),
        ("A", 15),
        ("C", 30),
        ("B", 25),
        ("A", 5),
        ("C", 40)
    ]
    
    # Custom mapper: extract category and value
    def sum_mapper(item):
        category, value = item
        return [(category, value)]
    
    # Custom reducer: sum values for each category
    def sum_reducer(category, values):
        total = sum(values)
        return (category, total)
    
    # Create MapReduce instance
    mr = SimpleMapReduce(num_mappers=2, num_reducers=2)
    
    # Run custom job
    results = mr.map_reduce(data, sum_mapper, sum_reducer)
    
    # Print results
    print("Results:")
    for category, total in sorted(results):
        print(f"  {category}: {total}")
    print()


if __name__ == "__main__":
    demo_word_count()
    demo_inverted_index()
    demo_custom_job()