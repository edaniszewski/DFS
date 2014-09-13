'''
The Chunk class is an object used to track and maintain the metadata
associated with a chunk.

Created on Aug 13, 2014

@author: erickdaniszewski
'''
import config

class Chunk(object):
    '''
    Contains the metadata associated with a chunk.

    chunkHandle: unique ID of the chunk
    chunkserverLocations: list of the chunkserves the chunk exists on
    length: current size of the chunk
    '''


    def __init__(self, chunkHandle, chunkserverLocations=[]):
        '''
        Constructor
        '''
        self.chunkHandle = chunkHandle
        self.chunkserverLocations = chunkserverLocations
        self.offset = 0
    
    def checkRemainingSize(self, sizeToAppend):
        '''
        Checks if a given size will fit on the chunk
        '''
        if (self.offset + sizeToAppend) > config.chunkSize:
            return False
        return True
            
            
            
