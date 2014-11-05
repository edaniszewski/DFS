"""
The net module defines the base classes for the networking capability
of the master server, the chunk server, and other network-related methods
used by the system.

Created on Aug 13, 2014

@author: erickdaniszewski
"""
import socket
import logging

import config


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("net_logger")


class UDP:
    """
    A UDP socket networking object
    """

    def __init__(self):
        self.socket = self.get_new_udp_socket_connection()

    def get_new_udp_socket_connection(self):
        """
        Create a new socket object, connect to the given host on the given port
        and return the socket object
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(config.heartbeat_timeout)
        # TODO: this seems weird, need to think about how to define ports/host for heartbeat udp
        s.bind((config.heartbeat_host, config.heartbeat_port))
        return s


    def send(self, sock, data):
        """
        Send data over a given socket
        """
        sock.sendto(data, (config.heartbeat_host, config.heartbeat_port))


    def receive(self, sock):
        """
        Receive data from a socket connection
        """
        # TODO: This is just a dummy receive for now. Will need to implement a real one once protocol is further developed.
        data = sock.recv(2048)
        return data


class ChunkServer():
    """
    Base server class for chunkservers. Handles all networking logic that chunkservers use.
    """

    def __init__(self):
        # FIXME: Are the chunkservers on the same port? They should be a different host
        # since they should be running from a seperate machine, but I suppose that shouldn't
        # matter too much.
        self.port = config.PORT
        self.host = config.HOST
        self.sock = None
        self.threads = set()

    def initialize_socket(self):
        """
        Initializes and binds a socket for the server on the host and port specified
        in the configuration file.
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            # TODO: LOG and provide means for graceful failure
            print "Unable to open socket: " + message
            print "Error value: " + str(value)


class MasterServer():
    """
    Base server class for the Master class. Handles all networking logic that the Master class
    uses. MasterServer is a multi-threaded TCP server.
    """

    def __init__(self):
        self.port = config.PORT
        self.host = config.HOST
        self.sock = None
        self.threads = set()

    def initialize_socket(self):
        """
        Initializes and binds a socket for the server on the host and port specified
        in the configuration file.
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            # TODO: LOG and provide means for graceful failure
            print "Unable to open socket: " + message
            print "Error value: " + str(value)