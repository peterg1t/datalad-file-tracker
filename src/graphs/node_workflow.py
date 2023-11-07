import time


class NodeWorkflow:
    """Base class of a node in the provenance trail (can be task or file)"""

    def __init__(self, commit, author, name, date, path, label, predecesor, ID):
        """Init method of the class

        Args:
            commit (str): _description_
        """
        self.commit = commit
        self.author = author
        self.name = name
        self.date = f"{time.asctime(time.gmtime(date))}"
        self.path = path
        self.predecesor = predecesor
        self.label = label
        self.ID = ID
