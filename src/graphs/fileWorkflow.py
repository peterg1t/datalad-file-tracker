import os
import base64
from graphs.nodeWorkflow import nodeWorkflow


class fileWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """

    def __init__(self, dataset, name, commit, fileBlob):
        super().__init__(commit)
        self.basename = os.path.basename(name)
        self.name = self._encode(self.basename)
        self.dataset = dataset
        self.fileBlob = fileBlob
        self.parentTask = []
        self.childTask = []
        self.type = "file"
        self.node_color = "red"
        self.literal_name = name

    def _encode(self, message):
        message_bytes = message.encode("ascii")
        base64_bytes = base64.b64encode(message_bytes)
        return base64_bytes.decode("ascii")  # byte64 message

    def _decode(self, base64_message):
        base64_bytes = base64_message.encode("ascii")
        message_bytes = base64.b64decode(base64_bytes)
        return message_bytes.decode("ascii")  # original message
