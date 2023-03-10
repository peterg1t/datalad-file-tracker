def line_process_file(line):
    list_line = line.split('<>')
    files = list_line[0].split(',')
    prec_nodes = list_line[1].split(',')
    
    return files, prec_nodes

def line_process_task(line):
    list_line = line.split('<>')
    task = list_line[0]
    command = list_line[1]
    prec_nodes = list_line[2].split(',')
    transform = list_line[3]

    return task, command, prec_nodes, transform


def remove_space(input):
    """! This function remove spaces in strings

    Args:
        input (str): A string

    Returns:
        str: The string without spaces
    """
    return "".join(input.split())

def file_name_expansion():
    pass
