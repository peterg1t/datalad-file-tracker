import os
from nodeWorkflow import nodeWorkflow

class taskWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """
    def __init__(self, name, graphInstanceID, abstractNodeIndex, concreteGraphID, commit, taskID):
        super().__init__(graphInstanceID, abstractNodeIndex, concreteGraphID, commit)
        self.name = name
        self.basename = name
        self.taskID = taskID
        self.parentFiles=[]
        self.childFiles=[]
        self.node_color = 'green'