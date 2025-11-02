"""
Sample jobs for the MapReduce framework
"""

def word_count_mapper(text_line):
    """
    Mapper function for word count job
    Takes a text line and returns list of (word, 1) tuples
    """
    words = text_line.split()
    return [(word.lower().strip('.,!?";'), 1) for word in words if word]

def word_count_reducer(word, counts):
    """
    Reducer function for word count job
    Takes a word and list of counts, returns (word, total_count)
    """
    return (word, sum(counts))

def inverted_index_mapper(line_with_file_info):
    """
    Mapper function for inverted index job
    Takes (filename, text) and returns (word, filename) tuples
    """
    filename, text = line_with_file_info
    words = text.split()
    return [(word.lower().strip('.,!?";'), filename) for word in words if word]

def inverted_index_reducer(word, filenames):
    """
    Reducer function for inverted index job
    Takes a word and list of filenames, returns (word, unique_filenames)
    """
    unique_files = list(set(filenames))
    return (word, unique_files)

def natural_join_mapper(table_row_with_id):
    """
    Mapper function for natural join job
    Takes (table_id, row) and returns (join_key, (table_id, row_data))
    """
    table_id, row = table_row_with_id
    # Assuming row is like (key, value1, value2, ...)
    key = row[0]  # First column is the join key
    return [(key, (table_id, row[1:]))]  # Return key and remaining columns

def natural_join_reducer(key, table_data_list):
    """
    Reducer function for natural join job
    Takes join key and list of (table_id, row_data), returns joined rows
    """
    table1_data = []
    table2_data = []
    
    for table_id, row_data in table_data_list:
        if table_id == 1:
            table1_data.extend([row_data] if not isinstance(row_data[0], list) else row_data)
        elif table_id == 2:
            table2_data.extend([row_data] if not isinstance(row_data[0], list) else row_data)
    
    # Generate joined results (cartesian product of matching rows)
    results = []
    for row1 in table1_data:
        for row2 in table2_data:
            results.append((key, *row1, *row2))
    
    return results