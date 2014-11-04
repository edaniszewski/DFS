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
import threading
import logging

from heartbeat import HeartbeatClient
from net import ChunkServer
from message import Message

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("chunkserver_logger")

class Chunkserver(ChunkServer):
    '''
    A server that holds the chunks that are the system storage unit. In addition to
    keeping handling the data that is associated with chunks, it provides chunk metadata
    to the master on request.
    '''
    def __init__(self):
        '''
        Constructor
        '''
        ChunkServer.__init__(self)
        self.check_chunkstore()
        self.heartbeat = HeartbeatClient()
        self.start_heartbeat()
        self.run()
        
        
    def check_chunkstore(self):
        '''
        Create the chunkstore directory if it does not yet exist
        '''
        log.info("Checking chunk storage")
        
        if not os.path.isdir(config.chunkstore):
            os.mkdir(config.chunkstore)
            log.info("Chunk storage created")
            
        log.info("Chunk storage found")
        
        
    def start_heartbeat(self):
        '''
        Broadcast a heartbeat message
        '''
        log.info("Starting chunkserver heartbeat")
        
        t = threading.Thread(target=self.heartbeat.ping_forever())
        t.daemon = True
        t.start()
        
        log.info("Chunkserver heartbeat started.")
        
        
    def run(self):
        '''
        Run the server. Initializes a socket and listens over it. Each incoming request is passed
        to a handler thread.
        '''
        log.info("Running chunkserver")
        
        self.initialize_socket()
        
        while True:
            sock, addr = self.sock.accept()

            t = threading.Thread(target=self.handle, args=(sock, addr))
            t.daemon = True
            t.start()
            self.threads.add(t)
            
        self.sock.close()
        for t in self.threads:
            t.join()
            
    
    def handle(self, sock, address):
        '''
        Method to handle incoming requests to the chunkserver
        '''
        log.info(threading.current_thread().name)
        data = sock.recv(1024)
        
        #========================================
        #
        # TODO: Implement parsing and delegation
        #
        #========================================
        
        if (data == Message.CREATE):
            #self.create_chunk(chunkHandle)
            pass
        
        elif (data == Message.APPEND):
            #self.append_chunk()
            pass
        
        elif (data == Message.DELETE):
            #self.delete_chunk(chunkHandle)
            pass
        
        elif (data == Message.READ):
            #self.read_chunk()
            pass
        
        elif (data == Message.WRITE):
            pass
        
        elif (data == Message.CONTENTS):
            pass
        
        elif (data == Message.CHUNKSPACE):
            pass
        
        else:
            log.warn("Message not recognized")
        
        
        #========================================
    
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
    
    

chunkserver = Chunkserver()