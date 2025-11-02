"""
CLI for the simplified MapReduce framework
"""
import argparse
from mapreduce import SimpleMapReduce
from jobs import word_count_mapper, word_count_reducer, inverted_index_mapper, inverted_index_reducer, natural_join_mapper, natural_join_reducer
import json


def run_word_count(input_file, num_mappers=1, num_reducers=1):
    """Run word count job"""
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # Remove empty lines and strip whitespace
    lines = [line.strip() for line in lines if line.strip()]
    
    mr = SimpleMapReduce(num_mappers, num_reducers)
    results = mr.map_reduce(lines, word_count_mapper, word_count_reducer)
    
    print("Word Count Results:")
    for word, count in sorted(results, key=lambda x: x[1], reverse=True):
        print(f"{word}: {count}")
    
    return results


def run_inverted_index(input_files, num_mappers=1, num_reducers=1):
    """Run inverted index job"""
    file_contents = []
    for file_path in input_files:
        with open(file_path, 'r') as f:
            content = f.read()
        file_contents.append((file_path, content))
    
    mr = SimpleMapReduce(num_mappers, num_reducers)
    results = mr.map_reduce(file_contents, inverted_index_mapper, inverted_index_reducer)
    
    print("Inverted Index Results:")
    for word, files in sorted(results):
        print(f"{word}: {files}")
    
    return results


def run_natural_join(table1_file, table2_file, num_mappers=1, num_reducers=1):
    """Run natural join job"""
    # Read table1
    with open(table1_file, 'r') as f:
        table1_lines = [line.strip() for line in f.readlines() if line.strip()]
    
    # Read table2
    with open(table2_file, 'r') as f:
        table2_lines = [line.strip() for line in f.readlines() if line.strip()]
    
    # Parse tables (assuming first row is header, data rows follow)
    table1_data = []
    for line in table1_lines[1:]:  # Skip header
        parts = [part.strip() for part in line.split(',')]
        table1_data.append((1, tuple(parts)))  # (table_id, row_data)
    
    table2_data = []
    for line in table2_lines[1:]:  # Skip header
        parts = [part.strip() for part in line.split(',')]
        table2_data.append((2, tuple(parts)))  # (table_id, row_data)
    
    # Combine both tables for processing
    all_data = table1_data + table2_data
    
    mr = SimpleMapReduce(num_mappers, num_reducers)
    results = mr.map_reduce(all_data, natural_join_mapper, natural_join_reducer)
    
    print("Natural Join Results:")
    for result in results:
        print(result)
    
    return results


def run_map_only(input_file, num_mappers=1):
    """Run map-only job"""
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # Remove empty lines and strip whitespace
    lines = [line.strip() for line in lines if line.strip()]
    
    mr = SimpleMapReduce(num_mappers)
    results = mr.map_only(lines, word_count_mapper)
    
    print("Map-Only Results:")
    for item in results:
        print(item)
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Simple MapReduce Framework')
    parser.add_argument('job_type', choices=['word_count', 'inverted_index', 'natural_join', 'map_only'], 
                       help='Type of job to run')
    parser.add_argument('--input', help='Input file or directory')
    parser.add_argument('--table1', help='Table 1 file for natural join')
    parser.add_argument('--table2', help='Table 2 file for natural join')
    parser.add_argument('--mappers', type=int, default=1, help='Number of mappers')
    parser.add_argument('--reducers', type=int, default=1, help='Number of reducers')
    
    args = parser.parse_args()
    
    if args.job_type == 'word_count':
        if not args.input:
            print("Input file required for word count job")
            return
        run_word_count(args.input, args.mappers, args.reducers)
    
    elif args.job_type == 'inverted_index':
        if not args.input:
            print("Input directory required for inverted index job")
            return
        # For this example, we'll just use a single file
        run_inverted_index([args.input], args.mappers, args.reducers)
    
    elif args.job_type == 'natural_join':
        if not args.table1 or not args.table2:
            print("Both table1 and table2 files required for natural join job")
            return
        run_natural_join(args.table1, args.table2, args.mappers, args.reducers)

    elif args.job_type == 'map_only':
        if not args.input:
            print("Input file required for map-only job")
            return
        run_map_only(args.input, args.mappers)


if __name__ == "__main__":
    main()