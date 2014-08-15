'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
from src.meta.globalstate import GlobalState
from src.meta.chunk import Chunk




class Master(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.globalState = GlobalState()
        self.currentChunk = Chunk(self.globalState.incrementChunkHandle())
        
        
    def createFile(self):
        pass
    
    def getUniqueChunkHandle(self):
        pass
    
    def createNewChunk(self):
        pass
    
    def linkChunkToFile(self):
        pass
    
    def append(self):
        pass
    
    def read(self):
        pass
    
    def delete(self):
        pass
    
    def undelete(self):
        pass
    
    def appendLock(self):
        pass
    
    def appendUnlock(self):
        pass
    
    def readLock(self):
        pass
    
    def readUnlock(self):
        pass
    
    def isAppendLocked(self):
        pass
    
    def isReadLocked(self):
        pass
    
    def isChunkEmpty(self):
        pass
    
    def deleteChunk(self):
        pass
    
    def getChunkLocations(self):
        pass
    
    def numberOfReplicas(self):
        pass
    
    def replicateChunk(self):
        pass
    
    def interrogateChunkserver(self):
        pass
    
    def markChunkserverInactive(self):
        pass
    
    def markChunkserverActive(self):
        pass
    
    def oplogRead(self):
        pass
    
    def oplogAppend(self):
        pass
    
    
    
    