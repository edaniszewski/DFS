"""
The Master class acts as the system metadata administrator and maintainer.
In addition to creating and updating metadata information, it also (or will also)
persist the data to allow for graceful recovery.

Note that the master does not handle the data associated with a file - only
the metadata associated with a file (and the chunks any file belongs to).

###############################################################################
The MIT License (MIT)

Copyright (c) 2014 Erick Daniszewski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
###############################################################################
"""
import os.path
from random import choice
import logging
import cPickle as pickle
import threading

import config
from net import MasterServer
from message import Message
from globalstate import GlobalState
import heartbeat


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("master_logger")


class Master(MasterServer):
    """
    Centralized administrator of system metadata. Initiates a global state to
    track the adding and updating of Chunks and Files. Includes (or will include) methods
    to persist global state.
    """
    def __init__(self, run=False):
        """
        Constructor

        :param run: Flag for debug, defaults to False. If True, skips initialization and server starting
        :return:
        """
        super(Master, self).__init__()
        self._m = Message()
        self.gs = GlobalState()
        if not run:
            self.initialize_master()
            self.initialize_heart_beat_listener()
            self.run()

    def initialize_master(self):
        """
        Executes a sequence of methods in order to properly set up the master server

        :rtype : None
        """
        log.info("Initializing master...")

        self.check_resources()
        self.restore_state()

        log.info("Master initialized successfully")

    @staticmethod
    def initialize_heart_beat_listener():
        """
        Initialize an instance of the heartbeat listener

        :rtype : None
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

        :rtype : object
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

        :rtype : object
        :param sock:
        :param address:
        """
        log.info(threading.current_thread().name)
        sysmsg = self.recv(sock)

        # ========================================
        #
        # TODO: Implement parsing and delegation
        # FIXME: Would it make sense to have this in a loop so one connection could
        # yield multiple actions? Or does a new connection have to be made for every
        # new action?
        #
        # ========================================

        if sysmsg == self._m.APPEND:
            #self.append(fileName, appendSize)
            log.info("Append received")

        elif sysmsg == self._m.READ:
            #self.read()
            log.info("Read received")

        elif sysmsg == self._m.SANITIZE:
            #self.sanitize()
            log.info("Sanitize received")

        elif sysmsg == self._m.DELETE:
            #self.delete()
            log.info("Delete received")

        elif sysmsg == self._m.UNDELETE:
            #self.undelete()
            log.info("Undelete received")

        elif sysmsg == self._m.CREATE:
            log.info("Create received")

        elif sysmsg == self._m.OPEN:
            log.info("Open received")

        elif sysmsg == self._m.CLOSE:
            log.info("Close received")

        elif sysmsg == self._m.WRITE:
            log.info("Write received")

        else:
            log.warn("Message not recognized.")

        #========================================

        # FIXME: Eventually, will want to send back some kind of message.
        sock.send("PLACEHOLDER")

    def get_current_chunk(self):
        """
        Gets the most recent chunk in the system. If initial start up,
        it first creates a chunk.

        :rtype : object
        """
        if self.gs.get_current_chunk == 0:
            self.gs.add_chunk(self.gs.get_next_chunk)
        return self.gs.get_chunk(self.gs.get_current_chunk)

    def global_state_snapshot(self):
        """
        Take a snapshot of the metadata and persist it to disk

        :rtype : object
        """
        # FIXME: Instead of replacing the previous snapshot, could have snapshots be incremental or
        # timestamped, and persist them in their own directory. This could allow you to return to
        # a state older than that of the previous snapshot. Since snapshots should be taken somewhat
        # frequently, it is possible that keeping only the past 10? 100? 1000? would be necessary.
        with open(config.metasnapshot, 'wb') as f:
            pickle.dump(self.gs, f)

    @staticmethod
    def check_resources():
        """
        Check to see if persisted resources exist from previous server instantiations.
        If the resources do not exist, create them. The resources which are checked are:

            TODO: Active hosts file may not be needed. May be replaced w/ snapshot depending on how heartbeat will work
            - active hosts file: persisted list of hosts
            - oplog file: persisted log of operations executed
            - metadata snapshot: a snapshot of the global metadata

        :rtype : object
        """
        if not os.path.isfile(config.activehosts):
            open(config.activehosts, 'w').close()
            log.warn("ACTIVE HOSTS FILE not found. Creating new active hosts file...")

        if not os.path.isfile(config.oplog):
            open(config.oplog, 'w').close()
            log.warn("OPLOG not found. Creating new oplog...")

    def restore_state(self):
        """
        Restore the master's global state to a previously pickled state. If loading in a
        pickled state was unsuccessful, log the error and create a new instance of global state.

        :rtype : object
        """
        if os.path.isfile(config.metasnapshot):
            try:
                self.gs = pickle.load(open(config.metasnapshot, 'rb'))

            except pickle.UnpickleableError:
                log.error("Unable to unpickle previously pickled global state. Creating new Global State instance.")

            except Exception as e:
                log.error("Unable to restore previous global state. Creating new Global State instance.\n{}".format(
                    e.message))
                raise e
        else:
            self.gs = GlobalState()

    def update_current_chunk(self):
        """
        When a chunk is filled, a new chunk will need to be created and
        the current chunk will need to be updated to the newly created chunk
        so further appends may continue.

        :rtype : object
        """
        pass

    def create_new_file(self, file_name):
        """
        Instantiate a new File object

        :rtype : object
        :param file_name:
        """
        self.gs.add_file(file_name)

    def create_new_chunk(self):
        """
        On CREATE or APPEND, master will create a new metadata Chunk
        Object to track the new chunk.

        :rtype : None
        """
        self.gs.add_chunk(self.gs.get_next_chunk())

    def link_chunk_to_file(self, chunk_handle, file_name):
        """
        When a new file is created, it needs to be associated with the
        chunk(s) that contain its data

        :rtype : object
        :param chunk_handle:
        :param file_name:
        """
        self.gs.get_file(file_name).chunk_handles.append(chunk_handle)

    def append(self, file_name, append_size):
        """
        Retrieves metadata necessary for an append to occur

        :rtype : object
        :param file_name:
        :param append_size:
        """
        try:
            chunk = self.gs.get_chunk(self.gs.get_file(file_name).chunk_handles[-1])
            chunk.update_offset(append_size)
        except:
            pass
        finally:
            self.global_state_snapshot()

    def read(self):
        """

        :rtype : object
        """
        try:
            pass
        except:
            pass
        finally:
            self.global_state_snapshot()

    def delete(self):
        """

        :rtype : object
        """
        try:
            pass
        except:
            pass
        finally:
            self.global_state_snapshot()

    def undelete(self):
        """

        :rtype : object
        """
        try:
            pass
        except:
            pass
        finally:
            self.global_state_snapshot()

    def append_lock(self):
        """
        Get a thread lock on a chunk to initiate a synchronous append on the chunk

        :rtype : object
        """
        pass

    def append_unlock(self):
        """
        Remove the thread lock on a chunk to allow other threads append access to the chunk

        :rtype : object
        """
        pass

    def read_lock(self):
        """
        Get a thread lock on a chink to initiate a synchronous read on the chunk

        :rtype : object
        """
        pass

    def read_unlock(self):
        """
        Remove the thread lock on a chunk to allow other threads read access to the chunk

        :rtype : object
        """
        pass

    def is_append_locked(self):
        """
        Check to see if a chunk has an active append lock

        :rtype : object
        """
        pass

    def is_read_locked(self):
        """
        Check to see if a chunk has an active read lock

        :rtype : object
        """
        pass

    def delete_chunk(self):
        """

        :rtype : object
        """
        pass

    def is_chunk_empty(self, chunk):
        """
        Check if a given chunk contains any data

        :rtype : object
        :param chunk:
        """
        if chunk.offset() == 0:
            return True
        return False

    def get_chunk_locations(self, chunk_handle):
        """
        Get the current locations that a chunk is stored at

        :rtype : list
        :param chunk_handle:
        """
        return self.gs.chunk_map[chunk_handle].chunkserverLocations

    def number_of_replicas(self, chunk_handle):
        """
        Get the current number of replicas of a specified chunk

        :rtype : int
        :param chunk_handle:
        """
        return len(self.get_chunk_locations(chunk_handle))

    def choose_chunk_locations(self, chunk_handle):
        """
        Choose locations for a chunk to be stored. Currently there is no
        load balancing in place to determine which chunkservers get chunks

        :rtype : object
        :param chunk_handle:
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
        """

        :rtype : object
        """
        pass

    def sanitize(self):
        """

        :rtype : object
        """
        pass


if __name__ == "__main__":
    master = Master()
