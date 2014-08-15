'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
from file import File

class GlobalState(object):
    '''
    Contains important global state, including the chunkHandle incrementor

    chunkHandle: unique ID of the chunks
    toDelete: list to hold filenames of files flagged for deletion. This increases
    the memory usage, but decreases the overhead of periodically searching for files
    flagged for deletion. Since deletion is not typically a frequent action, the memory
    overhead may be low. This still needs testing to verify.
    fileMap: key=filename value=file object
    chunkMap: key=chunkid value=[filename, filename, ...]
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.chunkHandle = 0
        self.toDelete = []
        self.fileMap = {}
        self.chunkMap = {}
        
        
    def incrementChunkHandle(self):
        '''
        Increments the chunk handle.
        '''
        self.chunkHandle += 1
        
        
    def incrementAndGetChunkHandle(self):
        '''
        Increment the chunk handle and return the new chunk handle
        '''
        self.incrementChunkHandle()
        return self.chunkHandle
    
    
    def addFile(self, filename):
        '''
        Add a new file object to the file map, keyed to the file name
        '''
        if filename not in self.fileMap.keys():
            self.fileMap[filename] = File(filename)
            return 1
        else:
            return 0
    
    
    def queueDelete(self, filename):
        '''
        Add a file to the tracked list of pending deletions
        '''
        try:
            self.toDelete.append(filename)
            return 1
        # TODO: Better exception handling
        except Exception:
            return 0
        
        
    def dequeueDelete(self, filename):
        '''
        Remove a file from the tracked list of pending deletions
        '''
        try:
            self.toDelete.remove(filename)
            return 1
        # TODO: Better exception handling
        except Exception:
            return 0
        
        
    def getFile(self, filename):
        '''
        Get a file object corresponding to a filename string
        '''
        try:
            return self.fileMap[filename]
        except Exception:
            return 0
        
    
        
    def getFiles(self):
        '''
        Get a list of all file objects in the system
        '''
        return self.fileMap.values()
    
    
    def getFileNames(self):
        '''
        Get a list of all file names currently used in the system
        '''
        return self.fileMap.keys()
    
    
    def cleanFileMap(self, filename):
        '''
        When a file is deleted, clean the map and remove its metadata footprint
        '''
        associatedChunks = self.fileMap[filename].chunkHandles
        
        for chunkHandle in associatedChunks:
            self.cleanChunkMap(chunkHandle)
            
        del self.fileMap[filename]
        
    
    def getChunk(self, chunkHandle):
        '''
        Get a chunk object corresponding to a chunkHandle
        '''
        return self.chunkMap[chunkHandle]
    
    
    def getChunks(self):
        '''
        Get a list of all chunk objects in the system
        '''
        return self.chunkMap.values()
    
    
    def getChunkIDs(self):
        '''
        Get a list of all the chunkHandles currently used in the system
        '''
        return self.chunkMap.keys()
    
    
    def cleanChunkMap(self, chunkHandle):
        '''
        When a file is deleted, its chunks must be deleted. Cleans the map and
        removes its metadata footprint
        '''
        del self.chunkMap[chunkHandle]
    
    
    