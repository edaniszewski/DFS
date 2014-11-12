"""
The Chunkserver class creates an instance of a chunkserver. Chunkservers act as the
storage managers of the system, keeping data (enclosed in chunks) on them. In addition
to coordinating with the client and managing the chunk data, it also provides certain
metrics and chunk metadata to the master on request (or will in the future).

Created on Aug 13, 2014

@author: erickdaniszewski
"""
import os
import threading
import logging
import struct

import config
from heartbeat import HeartbeatClient
from net import ChunkServer
from message import Message


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("chunkserver_logger")


class Chunkserver(object, ChunkServer):
    """
    A server that holds the chunks that are the system storage unit. In addition to
    keeping handling the data that is associated with chunks, it provides chunk metadata
    to the master on request.
    """

    def __init__(self):
        super(Chunkserver, self).__init__()
        self.check_chunkstore()
        self.heartbeat = HeartbeatClient()
        self.m = Message()
        self.chunk_set = set()
        self.rLock = threading.Lock()
        self.wLock = threading.Lock()
        self.start_heartbeat()
        self.run()

    @staticmethod
    def check_chunkstore():
        """
        Create the chunkstore directory if it does not yet exist

        :rtype : object
        """
        log.info("Validating chunk storage")

        if not os.path.isdir(config.chunkstore):
            os.mkdir(config.chunkstore)
            log.info("Chunk storage created")

        log.info("Chunk storage found")

    def start_heartbeat(self):
        """
        Broadcast a heartbeat message

        :rtype : None
        """
        log.info("Starting chunkserver heartbeat")

        t = threading.Thread(target=self.heartbeat.ping_forever())
        t.daemon = True
        t.start()

        log.info("Chunkserver heartbeat started.")

    def run(self):
        """
        Run the server. Initializes a socket and listens over it. Each incoming request is passed
        to a handler thread.

        :rtype : object
        """
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
        """
        Method to handle incoming requests to the chunkserver

        :rtype : object
        :param sock: 
        :param address: 
        """
        log.info("Handling request for thread " + threading.current_thread().name)
        sysmsg = self.recv(sock)

        # Request to create a new chunk
        if sysmsg == self.m.CREATE:
            chunk_handle = self.recv(sock)

            if not self.create_chunk(chunk_handle):
                self.send(sock, self.m.FAILURE)
            self.send(sock, self.m.SUCCESS)

        # Request to append data to an existing chunk
        elif sysmsg == self.m.APPEND:
            chunk_handle = self.recv(sock)
            data = self.recv(sock)

            if not self.append_chunk(chunk_handle, data):
                self.send(sock, self.m.FAILURE)
            self.send(sock, self.m.SUCCESS)

        # Request to permanently delete a chunk from the system
        elif sysmsg == self.m.DELETE:
            chunk_handle = self.recv(sock)

            if not self.delete_chunk(chunk_handle):
                self.send(sock, self.m.FAILURE)
            self.send(sock, self.m.SUCCESS)

        # Request to read from a chunk
        elif sysmsg == self.m.READ:
            chunk_handle = self.recv(sock)
            offset = self.recv(sock)
            size = self.recv(sock)
            data = self.read_chunk(chunk_handle, offset, size)
            
            if not data:
                self.send(sock, self.m.FAILURE)
            self.send(sock, data)

        # Request to write to a chunk
        elif sysmsg == self.m.WRITE:
            # data = struct.unpack("!L", self.recv(sock))[0]
            #if not self.write_chunk():
            #    self.send(sock, self.m.FAILURE)
            #self.send(sock, self.m.SUCCESS)
            pass

        # Request for chunks managed by the chunkserver
        elif sysmsg == self.m.CONTENTS:
            contents = self.get_contents()

            if not contents:
                self.send(sock, self.m.FAILURE)
            self.send(sock, contents)

        # Request for the amount of chunkstore space is left on the chunkserver
        elif sysmsg == self.m.CHUNKSPACE:
            chunk_handle = self.recv(sock)
            chunk_space = self.get_remaining_chunk_space(chunk_handle)

            if not chunk_space:
                self.send(sock, self.m.FAILURE)
            self.send(sock, chunk_space)

        else:
            log.warn("Message not recognized")

    def create_chunk(self, chunk_handle):
        """
        Create a file that will be the chunk

        :rtype : bool
        :param chunk_handle: Unique identifier for the chunk to be created.
        """
        try:
            open(config.chunkstore + str(chunk_handle), 'w').close()
            self.chunk_set.add(str(chunk_handle))
            return True
        except IOError:
            log.error("IOError when trying to create chunk " + str(chunk_handle))
            return False

    def append_chunk(self, chunk_handle, data):
        """
        Append data to a specified chunk

        :rtype : object
        :param chunk_handle: 
        :param data: 
        """
        try:
            with self.wLock:
                with open(config.chunkstore + str(chunk_handle), 'a') as f:
                    f.write(data)
                return True
        except IOError:
            log.error("Unable to append data to chunk " + str(chunk_handle))
            return False

    def read_chunk(self, chunk_handle, offset, size):
        """
        Read from a specified chunk

        :rtype : object
        :param chunk_handle:
        :param offset:
        :param size:
        """
        try:
            with self.rLock:
                with open(config.chunkstore + str(chunk_handle), 'r') as f:
                    f.seek(offset)
                    data = f.read(size)
                return data
        except IOError:
            log.error("Unable to read data from chunk " + str(chunk_handle))
            return False

    @staticmethod
    def delete_chunk(chunk_handle):
        """
        Deletes a chunk from the chunkstore

        :rtype : bool
        :param chunk_handle:
        """
        try:
            os.remove(config.chunkstore + str(chunk_handle))
            return True
        except IOError:
            log.error("Unable to delete chunk " + str(chunk_handle))
            return False

    def write_chunk(self):
        """
        Write data to a chunk

        :rtype : object
        """
        pass

    def get_contents(self):
        """
        Returns a list of the chunks that are stored on the chunkserver

        :rtype : list
        """
        return [str(chunk_handle) for chunk_handle in self.chunk_set]

    @staticmethod
    def get_remaining_chunk_space(chunk_handle):
        """
        Calculate and return the amount of available space left in a chunk

        :param chunk_handle:
        :rtype : str
        """
        return str(config.chunk_size - os.stat(config.chunkstore + str(chunk_handle)).st_size)


if __name__ == "__main__":
    chunkserver = Chunkserver()