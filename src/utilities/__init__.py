"""Init module for utilities."""
from .base_conversions import encode, decode  # pylint: disable=import-error
from .paths_check import (
    is_tool,
    exists_case_sensitive,
)  # pylint: disable=import-error
from .git_utils import (
    get_commit_list,
    commit_message_node_extract,
    get_dataset,
    get_superdataset,
    get_git_root,
    get_branches,
    sub_clone_flock,
    sub_get,
    sub_dead_here,
    sub_push_flock,
    job_checkout,
    git_merge,
    branch_save,
)  # pylint: disable=import-error
from .jobs import (
    job_submit,
    run_pending_nodes,
    job_prepare,
    job_clean,
)  # pylint: disable=import-error
from .string_manip import (
    line_process_file,
    line_process_task,
    line_process_task_v2,
    file_name_expansion,
    remove_space,
)  # pylint: disable=import-error


__all__ = [
    "is_tool",
    "exists_case_sensitive",
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
