import os
import csv

def translation_file_process(tf_filepath: str)
    node_mapping={}
    with open(tf_filepath, "r") as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{os.path.dirname(tf_filepath)}{row[1]}"
    
    return node_mapping