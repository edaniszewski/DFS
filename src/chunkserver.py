'''
The Chunkserver class creates an instance of a chunkserver. Chunkservers act as the 
storage managers of the system, keeping data (enclosed in chunks) on them. In addition
to coordinating with the client and managing the chunk data, it also provides certain 
metrics and chunk metadata to the master on request (or will in the future).

Created on Aug 13, 2014

@author: erickdaniszewski
'''
import config
import os
from heartbeat import HeartbeatClient

class Chunkserver(object):
    '''
    A server that holds the chunks that are the system storage unit. In addition to
    keeping handling the data that is associated with chunks, it provides chunk metadata
    to the master on request.
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.check_chunkstore()
        self.heartbeat = HeartbeatClient()
        
        
    def start_heartbeat(self):
        '''
        Broadcast a heartbeat message
        '''
        # TODO: This method should be called as a thread to prevent blocking
        self.heartbeat.ping_forever()
        
        
    def check_chunkstore(self):
        '''
        Create the chunkstore directory if it does not yet exist
        '''
        if not os.path.isdir(config.chunkstore):
            os.mkdir(config.chunkstore)
            
    
    def create_chunk(self, chunkHandle):
        '''
        Create a file that will be the chunk
        
        @param chunkHandle: the unique ID of the chunk
        '''
        open(config.chunkstore + str(chunkHandle), 'w').close()


    def append_chunk(self):
        pass


    def read_chunk(self):
        pass


    def delete_chunk(self, chunkHandle):
        '''
        Deletes a chunk from the chunkstore
        
        @param chunkHandle: the unique ID of the chunk
        '''
        os.remove(config.chunkstore + str(chunkHandle))
    
    

#FIXME: this is a temporary addition for quick testing until unit tests are written  
chunkserver = Chunkserver()