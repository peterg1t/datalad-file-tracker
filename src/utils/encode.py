import base64    
    
def encode(message):
    """A function to encode

    Args:
        message (str): A string for the message to encode

    Returns:
        str: A string of the encoded message
    """
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    return base64_bytes.decode('ascii') #byte64 message

def decode(base64_message):
    """A function to decode

    Args:
        message (str): A string for the message to decode

    Returns:
        str: A string of the decoded message
    """
    base64_bytes = base64_message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    return message_bytes.decode('ascii') #original message