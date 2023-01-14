import os
from nodeWorkflow import nodeWorkflow

class fileWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """
    def __init__(self, name, graphInstanceID, abstractNodeIndex, concreteGraphID, commit, fileBlob):
        super().__init__(graphInstanceID, abstractNodeIndex, concreteGraphID, commit)
        self.name = name
        self.basename = os.path.basename(name)
        self.fileBlob = fileBlob
        self.parentTask=[]
        self.childTask=[]
        self.node_color = 'red'