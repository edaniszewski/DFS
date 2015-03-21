"""
The global state object is the primary metadata storage object used by
the Master to maintain system state across all chunkservers.

Global state includes:
    - Mappings from file names to file objects
    - Mappings from chunk handles to chunk objects
    - A global chunk handle incrementor to provide unique chunkhandles
    - List of files marked for deletion
    - List of active chunk servers

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
import logging

from file import File
from chunk import Chunk
import config

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("global_state_logger")


class GlobalState(object):
    """
    Contains important global state, including the chunkHandle incrementer
    """
    def __init__(self):
        # FIXME: As per the GFS paper, chunk ids are immutable 64 bit UIDs
        self.c_lock = Lock()
        self._chunk_handle = 0
        self.to_delete = set()
        self.file_map = {}
        self.chunk_map = {}
        self.hosts = []
        self.active_hosts = []

    @property
    def get_current_chunk(self):
        """
        Gets the current chunk handle value but DOES NOT increment the chunk handle. This function should
        only be used as a check for the current chunk handle, not as a means to allocate a chunk handle to
        a new Chunk.

        :return: the current (most recent) chunk handle
        """
        return self._chunk_handle

    @property
    def get_next_chunk(self):
        """
        Increments the most current chunk handle and then returns the new chunk handle value. This should
        NOT be used to check chunk handles, but instead, it should be used to allocate chunk handles for
        new chunks.

        :return: a new chunk handle
        """
        with self.c_lock:
            self._chunk_handle += 1
        return self._chunk_handle

    def refresh_hosts(self):
        """
        Refresh the list of host IPs

        :rtype : object
        """
        with open(config.hosts, 'r') as f:
            self.hosts = f.read().splitlines()

    def refresh_active_hosts(self):
        """
        Refresh the list of active host IPs

        :rtype : object
        """
        with open(config.activehosts, 'r') as f:
            self.active_hosts = f.read().splitlines()

    def add_file(self, filename):
        """
        Add a new file object to the file map, keyed to the file name

        :rtype : object
        :param filename:
        """
        if filename not in self.file_map.keys():
            self.file_map[filename] = File(filename)
            return 1
        else:
            log.error("Filename '{}' already exists.".format(filename))
            return 0

    def queue_delete(self, filename):
        """
        Add a file to the tracked list of pending deletions

        :rtype : object
        :param filename:
        """
        if filename in self.file_map.keys():
            self.to_delete.add(filename)
            return 1
        else:
            return 0

    def dequeue_delete(self, filename):
        """
        Remove a file from the tracked list of pending deletions

        :rtype : object
        :param filename:
        """
        try:
            self.to_delete.remove(filename)
            return 1
        except KeyError:
            return 0

    def get_file(self, filename):
        """
        Get a file object corresponding to a filename string

        :rtype : File
        :param filename:
        """
        try:
            return self.file_map[filename]
        except KeyError:
            return None

    def get_files(self):
        """
        Get a list of all file objects in the system

        :rtype : object
        """
        return self.file_map.values()

    def get_file_names(self):
        """
        Get a list of all file names currently used in the system

        :rtype : object
        """
        return self.file_map.keys()

    def clean_file_map(self, filename):
        """
        When a file is deleted, clean the map and remove its metadata footprint

        :rtype : object
        :param filename:
        """
        associated_chunks = self.file_map[filename].chunk_handles

        for chunk_handle in associated_chunks:
            self.clean_chunk_map(chunk_handle)

        del self.file_map[filename]

    def add_chunk(self, chunk_handle):
        """
        Add a new chunk object to the chunk map, keyed to the chunk handle

        :rtype : object
        :param chunk_handle:
        """
        if chunk_handle not in self.chunk_map.keys():
            self.chunk_map[chunk_handle] = Chunk(chunk_handle)
            return 1
        else:
            return 0

    def get_chunk(self, chunk_handle):
        """
        Get a chunk object corresponding to a chunkHandle

        :rtype : Chunk
        :param chunk_handle:
        """
        try:
            return self.chunk_map[chunk_handle]
        except KeyError:
            log.error('Chunk handle {} does not exist in chunk map.'.format(chunk_handle))
            return None

    def get_chunks(self):
        """
        Get a list of all chunk objects in the system

        :rtype : object
        """
        return self.chunk_map.values()

    def get_chunk_ids(self):
        """
        Get a list of all the chunkHandles currently used in the system

        :rtype : object
        """
        return self.chunk_map.keys()

    def clean_chunk_map(self, chunk_handle):
        """
        When a file is deleted, its chunks must be deleted. Cleans the map and
        removes its metadata footprint

        :rtype : object
        :param chunk_handle:
        """
        try:
            del self.chunk_map[chunk_handle]
        except KeyError:
            log.error('Unable to delete chunk handle from map.')