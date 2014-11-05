"""
The Chunk class is an object used to track and maintain the metadata
associated with a chunk.

Created on Aug 13, 2014

@author: erickdaniszewski
"""
import config


class Chunk(object):
    """
    Contains the metadata associated with a chunk.
    """

    def __init__(self, chunk_handle, chunkserver_locations=[]):
        self.chunkHandle = chunk_handle
        self.chunkserver_locations = chunkserver_locations
        self.offset = 0

    def check_remaining_size(self, size_to_append):
        """
        Checks if a given size will fit on the chunk
        """
        if (self.offset + size_to_append) > config.chunk_size:
            return False
        return True