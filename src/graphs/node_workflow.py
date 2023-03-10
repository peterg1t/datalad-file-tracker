class NodeWorkflow:
    """Base class of a node in the provenance trail (can be task or file)"""

    def __init__(self, commit, name, path, label, ID):
        """Init method of the class

        Args:
            commit (str): _description_
        """
        self.commit = commit
        self.name = name
        self.path = path
        self.label = label
        self.ID = ID
