import re


"""This module will process line form the text file"""


def line_process_task(line):
    """This function will process a line from the text file

    Args:
        line (str): A line from the text file

    Returns:
        task_name: A task command
        task_command: A task command
        predecesors: A list of node predecesors
        transform: A transform for the task
    """
    line = line.rstrip()

    task_name = None
    task_command = None
    predecesors = None
    try:
        task_name = line.split("<>")[1]
        task_command = line.split("<>")[2]
        predecesors = line.split("<>")[3].split(",")
    except:  # pylint: disable = bare-except
        print("Incorrect file format, check the file and reload")

    try:
        transform = line.split("<>")[4]
    except:  # pylint: disable = bare-except
        print("No transform function has been defined")
        transform = ""

    return task_name, task_command, predecesors, transform


def line_process_file(line):
    """! Process a file line

    Args:
        line (str): A line corresponding to a file(s) node definition

    Returns:
        file_list: A list of file names for node creation
        predecesors: A list of predecesor nodes
    """
    line = line.rstrip()
    file_list = None
    predecesors = None
    try:
        file_list = line.split("<>")[1].split(",")
        predecesors = line.split("<>")[2].split(",")
    except:  # pylint: disable = bare-except
        print("Incorrect file format, check the file and reload")

    return file_list, predecesors


def remove_space(input):
    """! This function remove spaces in strings

    Args:
        input (str): A string

    Returns:
        str: The string without spaces
    """
    return "".join(input.split())


def file_name_expansion(item):
    """! This function will expand a file string in the form FILE{1..5}
    creating FILE1, FILE2,...

    Args:
        item (str): A string to expand

    Returns:
        files (list): A list of expanded files
    """
    files = []
    range_string = re.findall("(\d+\.\.\d+)", item)

    if range_string:
        start_range = int(range_string[0].split("..")[0])
        end_range = int(range_string[0].split("..")[1])

        for i in range(start_range, end_range + 1):
            file_splitted = re.split("(\{.*?\})", item)
            files.append(f"{file_splitted[0]}{i}{file_splitted[2]}")
    else:
        if len(item) == 0:
            pass
        else:
            files.append(item)

    return files
