# A MapReduce-like Framework (PDC Project)

A simplified implementation of the MapReduce programming model in Python. This framework demonstrates the core concepts of MapReduce with minimal complexity.

## Features

- Simple MapReduce execution model
- Configurable number of mappers and reducers
- Built-in job examples (Word Count, Inverted Index, Natural Join)
- Command-line interface for running jobs
- Easy to extend with custom jobs

## Architecture

The framework consists of:

- `SimpleMapReduce`: Core class that handles data partitioning, mapping, shuffling, and reducing
- `Jobs`: Predefined MapReduce job implementations
- `CLI`: Command-line interface to run jobs

## Usage

### Running the Example

```bash
python example.py
```

### Using the CLI

You can run the MapReduce jobs from the command line.

#### Running only the Map phase

To run only the map phase of a job (e.g., word count mapper), use the `map_only` job type:

```bash
# Map-only example for word count
python cli.py map_only --input sample_input.txt --mappers 2
```

This will execute only the mapping step and output the intermediate key-value pairs.

### Creating Custom Jobs

You can easily define your own MapReduce jobs by implementing mapper and reducer functions:

```python
def my_mapper(data_item):
    # Process a single data item and return list of (key, value) pairs
    return [(key, value), ...]

# Use with the framework
mr = SimpleMapReduce(num_mappers=2)
results = mr.map_reduce(data, my_mapper)
```

## Components

### SimpleMapReduce Class

The core class that orchestrates the MapReduce job:

1. Partitions input data across mappers
2. Applies the mapper function to each partition
3. Groups results by key (shuffle phase)
4. Applies the reducer function to each group

### Built-in Job

1. **Word Count**: Counts occurrences of each word in text

## Example Output

Running `python example.py` will show:

- Word Count results: each word and its occurrence count
