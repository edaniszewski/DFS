'''
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
'''
from meta.file import File
from meta.chunk import Chunk
import config



class GlobalState(object):
    '''
    Contains important global state, including the chunkHandle incrementor
    
    @var chunkHandle:    unique ID tracker for chunks
    @var toDelete:    set holding file names of the files that have been flagged for deletion
    @var fileMap:    dictionary with key=(str)filename, value=(File)file object
    @var chunkMap:    dictionary with key=(int)chunkHandle, value(Chunk)chunk object
    @var hosts:     list of chunkserver IP addresses
    @var activeHosts:    list of active chunkserver IP addresses
    '''
    def __init__(self):
        self._chunkHandle = 0
        self.toDelete = set()
        self.fileMap = {}
        self.chunkMap = {}
        self.hosts = []
        self.activeHosts = []
        
 
    def refresh_hosts(self):
        '''
        Refresh the list of host IPs
        '''
        with open(config.hosts, 'r') as f:
            self.hosts = f.read().splitlines()
    
    
    def refresh_active_hosts(self):
        '''
        Refresh the list of active host IPs
        '''
        with open(config.activehosts, 'r') as f:
            self.activeHosts = f.read().splitlines()
        
        
    def increment_chunk_handle(self):
        '''
        Increments the chunk handle.
        '''
        self._chunkHandle += 1
        
        
    def increment_and_get_chunk_handle(self):
        '''
        Increment the chunk handle and return the new chunk handle
        '''
        self.increment_chunk_handle()
        return self._chunkHandle
    
    
    def add_file(self, filename):
        '''
        Add a new file object to the file map, keyed to the file name
        '''
        if filename not in self.fileMap.keys():
            self.fileMap[filename] = File(filename)
            return 1
        else:
            return 0
    
    
    def queue_delete(self, filename):
        '''
        Add a file to the tracked list of pending deletions
        '''
        if filename in self.fileMap.keys():
            self.toDelete.add(filename)
            return 1
        else:
            return 0
        
        
    def dequeue_delete(self, filename):
        '''
        Remove a file from the tracked list of pending deletions
        '''
        try:
            self.toDelete.remove(filename)
            return 1
        # TODO: Better exception handling
        except Exception:
            return 0
        
        
    def get_file(self, filename):
        '''
        Get a file object corresponding to a filename string
        '''
        try:
            return self.fileMap[filename]
        except Exception:
            return 0
        
        
    def get_files(self):
        '''
        Get a list of all file objects in the system
        '''
        return self.fileMap.values()
    
    
    def get_file_names(self):
        '''
        Get a list of all file names currently used in the system
        '''
        return self.fileMap.keys()
    
    
    def clean_file_map(self, filename):
        '''
        When a file is deleted, clean the map and remove its metadata footprint
        '''
        associatedChunks = self.fileMap[filename].chunkHandles
        
        for chunkHandle in associatedChunks:
            self.clean_chunk_map(chunkHandle)
            
        del self.fileMap[filename]
        
        
    def add_chunk(self, chunkHandle):
        '''
        Add a new chunk object to the chunk map, keyed to the chunk handle
        '''
        if chunkHandle not in self.chunkMap.keys():
            self.chunkMap[chunkHandle] = Chunk(chunkHandle)
            return 1
        else:
            return 0
        
    
    def get_chunk(self, chunkHandle):
        '''
        Get a chunk object corresponding to a chunkHandle
        '''
        try:
            return self.chunkMap[chunkHandle]
        except Exception:
            return 0
        
    
    def get_chunks(self):
        '''
        Get a list of all chunk objects in the system
        '''
        return self.chunkMap.values()
    
    
    def get_chunk_ids(self):
        '''
        Get a list of all the chunkHandles currently used in the system
        '''
        return self.chunkMap.keys()
    
    
    def clean_chunk_map(self, chunkHandle):
        '''
        When a file is deleted, its chunks must be deleted. Cleans the map and
        removes its metadata footprint
        '''
        try:
            del self.chunkMap[chunkHandle]
        except Exception:
            raise RuntimeError('Unable to delete chunk handle from map.')


