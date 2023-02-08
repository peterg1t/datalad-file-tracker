class nodeWorkflow:
    """Base class of a node in the provenance trail (can be task or file)"""

    def __init__(self, commit):
        """Init method of the class

        Args:
            commit (str): _description_
        """
        self.commit = commit
        self.concrete_graph_ID = 0
