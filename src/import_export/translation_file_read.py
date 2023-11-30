"""Docstring translation file"""
import csv


def translation_file_process(tf_filepath: str):
    """This function will open a file containing the mapping for the file handles

    Args:
        tf_filepath (str): Path to the translation file

    Returns:
        dict: A mapping of nodes specifying the translation from file
        handle to file path
    """
    node_mapping = {}
    with open(tf_filepath, "r", encoding="utf-8") as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            # node_mapping[row[0]] = f"{os.path.dirname(tf_filepath)}/{row[1]}"
            node_mapping[row[0]] = row[1]

    return node_mapping
