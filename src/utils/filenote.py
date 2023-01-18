"""A class for a file note (node)
"""

class FileNote:
    """A class for a file note
    """
    def __init__(self, dataset, filename, relative, author, date, commit, summary, message): # pylint: disable=too-many-arguments
        self.filename = filename
        self.dataset = dataset
        self.author = author
        self.date = date
        self.relative = relative # child or parent of the file
        self.commit = commit #commit that created the file
        self.summary = summary
        self.message = message
