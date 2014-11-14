"""
The global state object is the primary metadata storage object used by
the Master to maintain system state across all chunkservers.

Global state includes:
    - Mappings from file names to file objects
    - Mappings from chunk handles to chunk objects
    - A global chunk handle incrementor to provide unique chunkhandles
    - List of files marked for deletion
    - List of active chunk servers

Created on Aug 13, 2014

@author: erickdaniszewski
"""
from file import File
from chunk import Chunk
import config


class GlobalState(object):
    """
    Contains important global state, including the chunkHandle incrementor
    """

    def __init__(self):
        # FIXME: As per the GFS paper, chunk ids are immutable 64 bit UIDs
        self._chunk_handle = 0
        self.to_delete = set()
        self.file_map = {}
        self.chunk_map = {}
        self.hosts = []
        self.active_hosts = []

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

    def increment_chunk_handle(self):
        """
        Increments the chunk handle.

        :rtype : object
        """
        self._chunk_handle += 1

    def increment_and_get_chunk_handle(self):
        """
        Increment the chunk handle and return the new chunk handle

        :rtype : object
        """
        self.increment_chunk_handle()
        return self._chunk_handle

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
        # TODO: Better exception handling
        except Exception:
            return 0

    def get_file(self, filename):
        """
        Get a file object corresponding to a filename string

        :rtype : object
        :param filename:
        """
        try:
            return self.file_map[filename]
        except Exception:
            return 0

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
        associated_chunks = self.file_map[filename].chunkHandles

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

        :rtype : object
        :param chunk_handle:
        """
        try:
            return self.chunk_map[chunk_handle]
        except Exception:
            return 0

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
        except Exception:
            raise RuntimeError('Unable to delete chunk handle from map.')