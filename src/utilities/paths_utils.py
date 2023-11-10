import os
import glob
from pathlib import Path



def is_tool(name):
    """! Check whether `name` is on PATH and marked as executable.

    Args:
        name (string): Name of the executable to search

    Returns:
        bool: True if the executable exists False otherwise
    """

    # from whichcraft import which
    from shutil import which

    return which(name) is not None


def exists_case_sensitive(path) -> bool:
    p = Path(path)
    if not p.exists():
        # If it already doesn't exist(),
        # we can skip iterdir().
        return False
    return p in p.parent.iterdir()

def full_path_from_partial(top_level_path: str, relative_path: str) -> str:
    """This function will return an absolute path from a patial path an a top level path

    Args:
        top_level_path (str): A top level path that contains the partial path
        relative_path (str): A partial path or file name

    Returns:
        str: An absolute path
    """
    return glob.glob(top_level_path + f"/**/*{os.path.basename(relative_path)}",recursive=True,)[0]
