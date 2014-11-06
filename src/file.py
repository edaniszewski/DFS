"""
The File class is an object used to track and maintain the
metadata associated with files within the system.

Created on Aug 13, 2014

@author: erickdaniszewski
"""


class File:
    """
    Contains the metadata associated with a file.
    """

    def __init__(self, file_name):
        # TODO: NAMESPACE implementation
        # TODO: FUTURE FEATURE: password-enabled files
        self.file_name = file_name
        self.chunk_handles = []
        self.delete = False
        self.size = None
        self.namespace = None
        self._password = None