import os
import base64
import glob
from nodeWorkflow import nodeWorkflow

class taskWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """
    def __init__(self, dataset, name, commit):
        super().__init__(commit)
        self.name = name
        self.basename = name
        self.dataset = dataset
        self.parentFiles=[]
        self.childFiles=[]
        self.type='task'
        self.node_color = 'green'

    def _encode(self, message):
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode('ascii') #byte64 message

    def _decode(self, base64_message):
        base64_bytes = base64_message.encode('ascii')
        message_bytes = base64.b64decode(base64_bytes)
        return message_bytes.decode('ascii') #original message

    def compute_id(self):
        parents_path=[]
        childs_path=[]

        if self.parentFiles:
            parents_path = [glob.glob(self.dataset+f"/**/*{os.path.basename(item)}", recursive=True)[0] for item in self.parentFiles]

        if self.childFiles:
            childs_path = [glob.glob(self.dataset+f"/**/*{os.path.basename(item)}", recursive=True)[0] for item in self.childFiles]

        self.id = self._encode('<>'.join([','.join(sorted(parents_path)), self.name, ','.join(sorted(childs_path))]))