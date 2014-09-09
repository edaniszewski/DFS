'''
The Master class acts as the system metadata administrator and maintainer. 
In addition to creating and updating metadata information, it also (or will also) 
persist the data to allow for graceful recovery.

Note that the master does not handle the data associated with a file - only
the metadata associated with a file (and the chunks any file belongs to). 

Created on Aug 13, 2014

@author: erickdaniszewski
'''
import os.path
from random import choice

import config, net
from meta.globalstate import GlobalState
import threading

try:
    import cPickle as pickle
except:
    import pickle


class Master(net.ThreadedTCPServer):
    '''
    MASTER - Centralized administrator of system metadata. Initiates a global state to
    track the adding and updating of Chunks and Files. Includes (or will include) methods
    to persist global state.
    '''


    def __init__(self):
        '''
        Constructor
        '''
        # Could use super, not sure if it matters here
        net.ThreadedTCPServer.__init__(self, (config.HOST, config.PORT), net.ThreadedTCPHandler)
        self.checkResources()
        self.restoreState()
        #FIXME: This is only the case for initial start up. Need algo to handle the case when the server is reset
        self.currentChunk = self.getCurrentChunk()
        self.startMasterServer()
        


    def getCurrentChunk(self):
        '''
        Gets the most recent chunk in the system. If initial start up, 
        it first creates a chunk.
        
        @return: Chunk object
        '''
        if self.gs.chunkHandle == 0:
            self.gs.addChunk(self.gs.incrementAndGetChunkHandle())
        return self.gs.getChunk(self.gs.chunkHandle)


    def stateSnapshot(self):
        '''
        Take a snapshot of the metadata and persist it to disk
        '''
        with open(config.metasnapshot, 'wb') as f:
            pickle.dump(self.gs, f)
        
        
    def startMasterServer(self):
        '''
        Start the server that the master will listen over
        '''
        server_thread = threading.Thread(target=self.serve_forever())
        server_thread.daemon=True
        server_thread.start()
        
        
    def checkResources(self):
        '''
        Check to make sure needed resources exist and take action if they do not
        '''
        #TODO: Print statements should be logged
        if not os.path.isfile(config.hosts):
            print "HOSTS FILE: {}, not found.\nExiting...".format(config.hosts)
            exit(-1)
        
        if not os.path.isfile(config.activehosts):
            open(config.activehosts, 'w').close()
            print "ACTIVE HOSTS FILE not found. Creating new active hosts file..."
            
        if not os.path.isfile(config.oplog):
            open(config.oplog, 'w').close()
            print "OPLOG not found. Creating new oplog..."
            
        if not os.path.isfile(config.metasnapshot):
            open(config.metasnapshot, 'w').close()
            print "Snapshot persistence file not found. Creating new snapshot file..."

       
    def getState(self):
        '''
        Load a previously pickled state
        '''
        state = pickle.load(open(config.metasnapshot, 'rb'))
        
        if not state:
            return GlobalState()
        
        return state

    
    def restoreState(self):
        '''
        Restore the master's global state to a previouly pickled state
        '''
        self.gs = self.getState()

        
    def updateCurrentChunk(self):
        '''
        When a chunk is filled, a new chunk will need to be created and 
        the current chunk will need to be updated to the newly created chunk 
        so further appends may continue.
        '''
        pass
        
        
    def createNewFile(self, fileName):
        '''
        On CREATE, master will create instantiate a new metadata File 
        Object to track the new file.
        
        @param fileName: The name of the file to be created
        '''
        self.gs.addFile(fileName)
    
    
    def createNewChunk(self):
        '''
        On CREATE or APPEND, master will create a new metadata Chunk
        Object to track the new chunk.
        '''
        chunkHandle = self.gs.incrementAndGetChunkHandle()
        self.gs.addChunk(chunkHandle)
    
    
    def linkChunkToFile(self, chunkHandle, fileName):
        '''
        When a new file is created, it needs to be associated with the
        chunk(s) that contain its data
        
        @param chunkHandle: the unique ID of the chunk
        @param fileName: the name of the file to be associated with the chunk
        '''
        f = self.gs.getFile(fileName)
        f.chunkHandles.append(chunkHandle)
    
    
    def append(self, fileName, appendSize):
        '''
        Retrieves metadata necessary for an append to occur
        
        @param fileName: the name of the file being appended to
        @param appendSize: amount of data (in bytes) to be appended to the chunk
        '''
        curChunk = self.currentChunk
        if curChunk.offset + appendSize < config.chunkSize:
            self.linkChunkToFile(curChunk.chunkHandle(), fileName)
        else:
            print "TMP MSG: can not append -- not enough space in chunk"
    
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
    
    
    def getChunkLocations(self, chunkHandle):
        '''
        Get the current locations that a chunk is stored at
        
        @param chunkHandle: the unique ID of the chunk
        @return: (list) the IP addresses of the chunkservers the chunk is stored on
        '''
        return self.gs.chunkMap[chunkHandle].chunkserverLocations
    
    
    def numberOfReplicas(self, chunkHandle):
        '''
        Get the current number of replicas of a specified chunk
        
        @param chunkHandle: the unique ID of the chunk
        @return: (int) number of locations chunk is stored at
        '''
        return len(self.getChunkLocations(chunkHandle))
    
    
    def chooseChunkLocations(self, chunkHandle):
        '''
        Choose locations for a chunk to be stored. Currently there is no
        load balancing in place to determine which chunkservers get chunks
        
        @param currentLocations: the locations the chunk is currently stored
        '''
        #TODO: Implement load balancing
        
        currentLocations = self.getChunkLocations(chunkHandle)
        numOfLocs = self.numberOfReplicas(chunkHandle)
        # In the case that 3 replicas exist, 
        if numOfLocs > config.replicaAmount:
            return
        else:
            amntLocNeeded = config.replicaAmount - numOfLocs
            activeHosts = self.gs.activeHosts
            newLocations = []
            
            # Remove the locations the chunk already occupies
            for host in currentLocations:
                activeHosts.remove(host)
            
            # Choose new locations for the chunk
            for num in amntLocNeeded:  # @UnusedVariable
                loc = choice(activeHosts)
                newLocations.append(loc)
                activeHosts.remove(loc)
                
            return newLocations

    
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
    
    
    
#FIXME: this is a temporary addition for quick testing until unit tests are written  
if __name__=="__main__":
    master = Master()
    

    