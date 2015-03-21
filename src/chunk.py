"""
The Chunk class is an object used to track and maintain the metadata
associated with a given chunk. The Global State, instantiated by the
Master, contains the associations between these chunks, which represent
chunks of data on a chunk server, and the files which they compose.

The three notable components of chunk metadata are:

1. The chunk handle:
    The chunk handle is the unique identifier for the given chunk. In
    this current implementation, it is an integer value.

2. The chunk locations:
    The chunk locations are the addresses of the chunk servers which
    the chunk exists on.

3. The chunk offset:
    The chunk offset is the byte offset within the chunk which has
    already been written to. That is to say, if a chunk already contains
    some data, but is not filled, and additional data is appended to the
    chunk, the new append would begin at this offset.

###############################################################################
The MIT License (MIT)

Copyright (c) 2014 Erick Daniszewski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
###############################################################################
"""
from threading import Lock

import config


class Chunk(object):
    """
    Contains the metadata associated with a chunk.
    """
    def __init__(self, chunk_handle, chunkserver_locations=None):
        if not chunkserver_locations:
            chunkserver_locations = []

        self.chunk_handle = chunk_handle
        self.chunkserver_locations = chunkserver_locations
        self._offset = 0
        self.lock = Lock()

    def check_remaining_size(self, size_to_append):
        """
        Checks if a given size will fit within the remaining space of the chunk

        :rtype : bool
        :param size_to_append: the size of the data to be appended
        """
        return (self._offset + size_to_append) <= config.chunk_size

    def update_offset(self, size):
        """
        Updates the offset of the chunk. This method contains locking to prevent asynchronous overwrites

        :rtype : None
        :param size: The size to update the offset by
        """
        with self.lock:
            self._offset += size