"""This module provides utilities for path manipulations"""
import glob
import os
from pathlib import Path
from shutil import which


def is_tool(name):
    """! Check whether `name` is on PATH and marked as executable.

    Args:
        name (string): Name of the executable to search

    Returns:
        bool: True if the executable exists False otherwise
    """
    return which(name) is not None


def exists_case_sensitive(path) -> bool:
    """Checks if a specified path exists and is a direct child of its
    parent directory.

    Args:
    path (str or Path): The path to be checked.

    Returns:
    bool: Returns True if the specified path exists and is a direct child of
    its parent directory,
          otherwise returns False.
    """
    designated_path = Path(path)
    if not designated_path.exists():
        # If it already doesn't exist(),
        # we can skip iterdir().
        return False
    return designated_path in designated_path.parent.iterdir()


def full_path_from_partial(top_level_path: Path, relative_path: Path) -> Path:
    """This function will return an absolute path from a partial path an a
    top level path

    Args:
        top_level_path (str): A top level path that contains the partial path
        relative_path (str): A partial path or file name

    Returns:
        str: An absolute path
    """
    absolute_path_from_partial_path = glob.glob(top_level_path + f"/**/*{os.path.basename(relative_path)}", recursive=True)
    if absolute_path_from_partial_path:
        return absolute_path_from_partial_path[0]

    raise FileNotFoundError("File does not exists in the project")
