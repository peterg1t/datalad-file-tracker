import git

def get_git_root(path_file):
    """! This function will get the git repo of a file
    Args:
        path_initial_file (str): A path to the initial file
    Returns:
        str: The root of the git repo
    """
    git_repo = git.Repo(path_file, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    
    return git_root