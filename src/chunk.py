"""
The Chunk class is an object used to track and maintain the metadata
associated with a chunk.

Created on Aug 13, 2014

@author: erickdaniszewski
"""
from threading import Lock

import config


class Chunk(object):
    """
    Contains the metadata associated with a chunk.
    """

    def __init__(self, chunk_handle, chunkserver_locations=[]):
        self.chunk_handle = chunk_handle
        self.chunkserver_locations = chunkserver_locations
        self.offset = 0
        self.lock = Lock()

    def check_remaining_size(self, size_to_append):
        """
        Checks if a given size will fit on the chunk

        :rtype : object
        :param size_to_append:
        """
        if (self.offset + size_to_append) > config.chunk_size:
            return False
        return True

    def update_offset(self, size):
        """
        Updates the offset of the chunk. This method is locked to prevent asynchronous overwrites

        :rtype : object
        :param size:
        """
        with self.lock:
            self.offset += size