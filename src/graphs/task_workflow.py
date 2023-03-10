import os
import base64
from graphs.node_workflow import NodeWorkflow


class TaskWorkflow(NodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """

    def __init__(self, dataset, command, commit):
        super().__init__(commit, command, "", "", "")
        self.dataset = dataset
        self.cmd = command
        self.transform = ""
        self.parentFiles = []
        self.childFiles = []
        self.type = "task"
        self.node_color = "green"

    def _encode(self, message):
        message_bytes = message.encode("ascii")
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode("ascii")  # byte64 message

    def _decode(self, base64_message):
        base64_bytes = base64_message.encode("ascii")
        message_bytes = base64.b64decode(base64_bytes)
        return message_bytes.decode("ascii")  # original message

    def compute_id(self):
        """This function will compute the ID and store it in the name attribute of the object"""
        parents_path = []
        childs_path = []

        if self.parentFiles:
            # parents_path = [os.path.basename(glob.glob(self.dataset+f"/**/*{os.path.basename(item)}", recursive=True)[0]) for item in self.parentFiles] # Just in case we need the full path in the future
            parents_path = [os.path.basename(item) for item in self.parentFiles]

        if self.childFiles:
            # childs_path = [os.path.basename(glob.glob(self.dataset+f"/**/*{os.path.basename(item)}", recursive=True)[0]) for item in self.childFiles] # Just in case we need the full path in the future
            childs_path = [os.path.basename(item) for item in self.childFiles]
