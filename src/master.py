"""
The Master class acts as the system metadata administrator and maintainer.
In addition to creating and updating metadata information, it also (or will also)
persist the data to allow for graceful recovery.

Note that the master does not handle the data associated with a file - only
the metadata associated with a file (and the chunks any file belongs to).

Created on Aug 13, 2014

@author: erickdaniszewski
"""
import os.path
from random import choice
import logging
import cPickle as pickle
import threading

import config
from net import MasterServer
from message import Message
from src.globalstate import GlobalState
import heartbeat


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("master_logger")


class Master(MasterServer):
    """
    Centralized administrator of system metadata. Initiates a global state to
    track the adding and updating of Chunks and Files. Includes (or will include) methods
    to persist global state.
    """

    def __init__(self):
        MasterServer.__init__(self)
        self.m = Message()
        self.currentChunk = None
        self.gs = None
        self.initialize_master()
        self.initialize_heart_beat_listener()
        self.run()

    def initialize_master(self):
        """
        Executes a sequence of methods in order to properly set up the master server
        """
        log.info("Initializing master...")

        self.check_resources()
        self.restore_state()
        # FIXME: This is only the case for initial start up. Need to also handle the case when the server is reset
        self.currentChunk = self.get_current_chunk()

        log.info("Master initialized successfully")

    def initialize_heart_beat_listener(self):
        """
        Initialize an instance of the heartbeat listener
        """
        log.info("Initializing heartbeat listener...")

        listener = heartbeat.HeartbeatListener()
        listener.daemon = True
        listener.start()

        log.info('Heartbeat listener initialized successfully')

    def run(self):
        """
        Run the server. Initializes a socket and listens over it. Each incoming request is passed
        to a handler thread.
        """
        log.info("Running master server")
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
        """
        Method called by the run method when the server receives an incoming request. The request
        is parsed and delegated out accordingly.
        """
        log.info(threading.current_thread().name)
        data = sock.recv(1024)

        # ========================================
        #
        # TODO: Implement parsing and delegation
        #
        #========================================

        if data == self.m.APPEND:
            #self.append(fileName, appendSize)
            pass

        elif data == self.m.READ:
            #self.read()
            pass

        elif data == self.m.SANITIZE:
            #self.sanitize()
            pass

        elif data == self.m.DELETE:
            #self.delete()
            pass

        elif data == self.m.UNDELETE:
            #self.undelete()
            pass

        elif data == self.m.CREATE:
            pass

        elif data == self.m.OPEN:
            pass

        elif data == self.m.CLOSE:
            pass

        elif data == self.m.WRITE:
            pass

        else:
            log.warn("Message not recognized.")

        #========================================

        # FIXME: Eventually, will want to send back some kind of message.
        sock.send("PLACEHOLDER")

    def get_current_chunk(self):
        """
        Gets the most recent chunk in the system. If initial start up,
        it first creates a chunk.

        @return: Chunk object
        """
        if self.gs.chunkHandle == 0:
            self.gs.add_chunk(self.gs.increment_and_get_chunk_handle())
        return self.gs.get_chunk(self.gs.chunkHandle)

    def state_snapshot(self):
        """
        Take a snapshot of the metadata and persist it to disk
        """
        # FIXME: Instead of replacing the previous snapshot, could have snapshots be incremental or
        # timestamped, and persist them in their own directory. This could allow you to return to 
        # a state older than that of the previous snapshot. Since snapshots should be taken somewhat
        # frequently, it is possible that keeping only the past 10? 100? 1000? would be necessary. 
        with open(config.metasnapshot, 'wb') as f:
            pickle.dump(self.gs, f)

    def check_resources(self):
        """
        Check to see if persisted resources exist from previous server instantiations.
        If the resources do not exist, create them. The resources which are checked are:

            TODO: Active hosts file may not be needed. May be replaced w/ snapshot depending on how heartbeat will work
            - active hosts file: persisted list of hosts
            - oplog file: persisted log of operations executed
            - metadata snapshot: a snapshot of the global metadata
        """
        if not os.path.isfile(config.activehosts):
            open(config.activehosts, 'w').close()
            log.warn("ACTIVE HOSTS FILE not found. Creating new active hosts file...")

        if not os.path.isfile(config.oplog):
            open(config.oplog, 'w').close()
            log.warn("OPLOG not found. Creating new oplog...")

        if not os.path.isfile(config.metasnapshot):
            open(config.metasnapshot, 'w').close()
            log.warn("Snapshot persistence file not found. Creating new snapshot file...")

    def load_global_state(self):
        """
        Load in a pickled global state (from meta.snapshot resource)
        """
        state = pickle.load(open(config.metasnapshot, 'rb'))
        # If nothing was loaded in, create a new instance of GlobalState
        if not state:
            return GlobalState()
        # Otherwise, return the loaded state
        return state

    def restore_state(self):
        """
        Restore the master's global state to a previously pickled state. If loading in a
        pickled state was unsuccessful or there was no pickled snapshot to load in, restore_state()
        will instantiate a new instance of GlobalState.
        """
        self.gs = self.load_global_state()

    def update_current_chunk(self):
        """
        When a chunk is filled, a new chunk will need to be created and
        the current chunk will need to be updated to the newly created chunk
        so further appends may continue.
        """
        pass

    def create_new_file(self, file_name):
        """
        Instantiate a new File object
        """
        self.gs.addFile(file_name)

    def create_new_chunk(self):
        """
        On CREATE or APPEND, master will create a new metadata Chunk
        Object to track the new chunk.
        """
        chunk_handle = self.gs.incrementAndGetChunkHandle()
        self.gs.addChunk(chunk_handle)

    def link_chunk_to_file(self, chunk_handle, file_name):
        """
        When a new file is created, it needs to be associated with the
        chunk(s) that contain its data
        """
        f = self.gs.getFile(file_name)
        f.chunkHandles.append(chunk_handle)

    def append(self, file_name, append_size):
        """
        Retrieves metadata necessary for an append to occur
        """
        current_chunk = self.currentChunk
        if current_chunk.offset + append_size < config.chunk_size:
            self.link_chunk_to_file(current_chunk.chunkHandle(), file_name)
        else:
            log.info("Can not append -- not enough space in chunk")

    def read(self):
        pass

    def delete(self):
        pass

    def undelete(self):
        pass

    def append_lock(self):
        """
        Get a thread lock on a chunk to initiate a synchronous append on the chunk
        """
        pass

    def append_unlock(self):
        """
        Remove the thread lock on a chunk to allow other threads append access to the chunk
        """
        pass

    def read_lock(self):
        """
        Get a thread lock on a chink to initiate a synchronous read on the chunk
        """
        pass

    def read_unlock(self):
        """
        Remove the thread lock on a chunk to allow other threads read access to the chunk
        """
        pass

    def is_append_locked(self):
        """
        Check to see if a chunk has an active append lock
        """
        pass

    def is_read_locked(self):
        """
        Check to see if a chunk has an active read lock
        """
        pass

    def delete_chunk(self):
        pass

    def is_chunk_empty(self, chunk):
        """
        Check if a given chunk contains any data
        """
        if chunk.offset() == 0:
            return True
        return False

    def get_chunk_locations(self, chunk_handle):
        """
        Get the current locations that a chunk is stored at
        """
        return self.gs.chunk_map[chunk_handle].chunkserverLocations

    def number_of_replicas(self, chunk_handle):
        """
        Get the current number of replicas of a specified chunk
        """
        return len(self.get_chunk_locations(chunk_handle))

    def choose_chunk_locations(self, chunk_handle):
        """
        Choose locations for a chunk to be stored. Currently there is no
        load balancing in place to determine which chunkservers get chunks
        """
        # TODO: Implement load balancing

        current_locations = self.get_chunk_locations(chunk_handle)
        num_of_locs = self.number_of_replicas(chunk_handle)
        # In the case that an appropriate number of replicas exist, 
        if num_of_locs >= config.replica_amount:
            return
        else:
            amnt_of_locs_needed = config.replica_amount - num_of_locs
            active_hosts = self.gs.active_hosts
            new_locations = []

            # Remove the locations the chunk already occupies
            for host in current_locations:
                active_hosts.remove(host)

            # Choose new locations for the chunk
            for num in range(amnt_of_locs_needed):
                loc = choice(active_hosts)
                new_locations.append(loc)
                active_hosts.remove(loc)

            return new_locations

    def replicate_chunk(self):
        pass

    def sanitize(self):
        pass

# FIXME: this is a temporary addition for quick testing until unit tests are written
if __name__ == "__main__":
    master = Master()