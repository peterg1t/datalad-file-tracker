class nodeWorkflow:
    """ Base class of a node in the provenance trail (can be task or file)
    """
    def __init__(self, graphInstanceID, abstractNodeIndex, concreteGraphID, commit):
        """ Init method of the class

        Args:
            graphInstanceID (str): The graph instance
            abstractNodeIndex (str): _description_
            concreteGraphID (str): _description_
            commit (str): _description_
        """
        self.graphInstanceID = graphInstanceID
        self.abstractNodeIndex = abstractNodeIndex
        self.concreteGraphID = concreteGraphID
        self.commit = commit
    