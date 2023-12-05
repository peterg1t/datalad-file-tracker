"""This module will process line form the text file"""
import re


def line_process_task(line):
    """This function will process a line from the text file

    Args:
        line (str): A line from the text file

    Returns:
        task_name: A task command
        task_command: A task command
        predecessors: A list of node predecessors
        workflow: The workflow to which the task belongs, if void workflow="main"
    """
    line = line.rstrip()

    task_name = None
    task_command = None
    predecessors = None
    try:
        task_name = line.split("<>")[1]
        predecessors = line.split("<>")[2].split(",")
        task_command = line.split("<>")[3]
        workflow = line.split("<>")[4]
    except ValueError as error:  # pylint: disable = bare-except
        print(f"Incorrect file format, check the file and reload {error}")

    if not workflow:
        workflow = "main"

    # print(f"processing node {task_name}, workflow is {workflow}")

    return task_name, predecessors, task_command, workflow


def line_process_task_v2(line):
    """This function will process a line from the text file

    Args:
        line (str): A line from the text file

    Returns:
        task_name: A task command
        task_command: A task command
        predecessors: A list of node predecessors
        workflow: The workflow to which the task belongs, if void
        workflow="main"
    """
    line = line.rstrip()

    try:
        task_name = line.split("<>")[1]
        inputs = line.split("<>")[2].split(",")
        outputs = line.split("<>")[3].split(",")
        task_command = line.split("<>")[4]
        pce = line.split("<>")[5]
        subworkflow = line.split("<>")[6]
        message = line.split("<>")[7]
    except ValueError as error:  # pylint: disable = bare-except
        print(f"Incorrect file format, check the file and reload {error}")

    if not subworkflow:
        subworkflow = "main"

    # print(f"processing node {task_name}, workflow is {workflow}")

    return task_name, inputs, outputs, task_command, pce, subworkflow, message


def line_process_file(line):
    """! Process a file line

    Args:
        line (str): A line corresponding to a file(s) node definition

    Returns:
        file_list: A list of file names for node creation
        predecessors: A list of predecessor nodes
    """
    line = line.rstrip()
    file_list = None
    predecessors = None
    try:
        file_list = line.split("<>")[1].split(",")
        predecessors = line.split("<>")[2].split(",")
    except ValueError as error:  # pylint: disable = bare-except
        print(f"Incorrect file format, check the file and reload {error}")

    return file_list, predecessors


def remove_space(input_string):
    """! This function remove spaces in strings

    Args:
        input (str): A string

    Returns:
        str: The string without spaces
    """
    return "".join(input_string.split())


def file_name_expansion(item):
    """! This function will expand a file string in the form FILE{1..5}
    creating FILE1, FILE2,...

    Args:
        item (str): A string to expand

    Returns:
        files (list): A list of expanded files
    """
    files = []
    range_string = re.findall(r"(\d+\.\.\d+)", item)

    if range_string:
        start_range = int(range_string[0].split("..")[0])
        end_range = int(range_string[0].split("..")[1])

        for i in range(start_range, end_range + 1):
            file_splitted = re.split(r"(\{.*?\})", item)
            files.append(f"{file_splitted[0]}{i}{file_splitted[2]}")
    else:
        if len(item) == 0:
            pass
        else:
            files.append(item)

    return files
