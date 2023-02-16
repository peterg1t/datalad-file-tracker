import shutil


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
