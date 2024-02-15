"""This module will contain functions for git operations"""
from pathlib import Path
import subprocess


def globus_transfer(src_collection: str,
                    dest_collection: str,
                    file: Path,
                    dest_dir: str,
                    label: str) -> None:
    """This function will import a file bundle from a file into a specified dataset.

    Args:
        path_dataset (Path): The path to the dataset
        path_bundle (Path): The path to the bundle
    """
    import os
    import time
    import subprocess
    outlogs = []
    errlogs = []
    transfer_command = f'globus transfer "{src_collection}:{file}" "{dest_collection}:{dest_dir}/{os.path.basename(file)}" \
    --label "{label}"'
    transfer_command_output = subprocess.run(
        transfer_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = transfer_command_output.stdout.split("\n")
    errlog = transfer_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)

    return ("logs", outlog, errlog)
