'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''

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
        
    def getOffset(self):
        return self.offset