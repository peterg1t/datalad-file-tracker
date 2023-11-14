

def translation_file_process(tf_filepath: str)
    with open(tf_filepath, "r") as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_graph_path}{row[1]}"