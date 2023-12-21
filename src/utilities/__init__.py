"""Init module for utilities."""
from .base_conversions import decode, encode  # pylint: disable=import-error
from .git_utils import (  # pylint: disable=import-error
    branch_save,
    commit_message_node_extract,
    get_branches,
    get_commit_list,
    get_dataset,
    get_git_root,
    get_superdataset,
    git_merge,
    job_checkout,
    sub_clone_flock,
    sub_dead_here,
    sub_get,
    sub_push_flock,
)
from .jobs import (  # pylint: disable=import-error
    job_clean,
    job_prepare,
    job_submit,
    run_pending_nodes,
)
from .paths_utils import (  # pylint: disable=import-error
    exists_case_sensitive,
    full_path_from_partial,
    is_tool,
)
from .string_manip import (  # pylint: disable=import-error
    file_name_expansion,
    line_process_file,
    line_process_task,
    line_process_task_v2,
    remove_space,
)

import match

__all__ = [
    "is_tool",
    "exists_case_sensitive",
    "full_path_from_partial",
    "get_commit_list",
    "commit_message_node_extract",
    "get_dataset",
    "get_superdataset",
    "get_git_root",
    "get_branches",
    "sub_clone_flock",
    "sub_get",
    "sub_dead_here",
    "sub_push_flock",
    "job_checkout",
    "git_merge",
    "branch_save",
    "job_submit",
    "run_pending_nodes",
    "job_prepare",
    "job_clean",
    "line_process_file",
    "line_process_task",
    "line_process_task_v2",
    "file_name_expansion",
    "remove_space",
    "encode",
    "decode",
]
