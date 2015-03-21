"""
The net module defines the base classes for the networking capability
of the master server, the chunk server, and other network-related methods
used by the system.

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
import socket
import logging
import struct

import config


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("net_logger")


class BaseServer(object):
    """
    A class to act as the base server for the ChunkServer and MasterServer classes.
    Contains methods for network send and recieve.
    """
    def __init__(self):
        self.struct = struct.Struct('!L')

    def send(self, socket, data):
        """
        Send method which first sends the length of the data

        :rtype : object
        :param socket:
        :param data:
        """
        data_length = len(data)
        socket.send(self.struct.pack(data_length))

        total_sent = 0
        while total_sent < data_length:
            sent = socket.send(data[total_sent:])
            if not sent:
                raise RuntimeError("Socket connection was broken.")
            total_sent += sent

    def recv(self, socket):
        """
        Receive method which first gets the length of the file to receive

        :rtype : str
        :param socket:
        """
        data = ""
        data_length = self.struct.unpack(socket.recv(4))[0]
        while len(data) < data_length:
            data += socket.recv(data_length - len(data))

        return data


class UDP(object):
    """
    A UDP socket networking object
    """

    def __init__(self):
        self.socket = self.get_new_udp_socket_connection()

    def get_new_udp_socket_connection(self):
        """
        Create a new socket object, connect to the given host on the given port
        and return the socket object

        :rtype : object
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(config.heartbeat_timeout)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # TODO: this seems weird, need to think about how to define ports/host for heartbeat udp
        s.bind((config.heartbeat_host, config.heartbeat_port))
        return s

    def send(self, sock, data):
        """
        Send data over a given socket

        :rtype : object
        :param sock:
        :param data:
        """
        sock.sendto(data, (config.heartbeat_host, config.heartbeat_port))

    def receive(self, sock):
        """
        Receive data from a socket connection

        :rtype : object
        :param sock:
        """
        # TODO: This is just a dummy receive for now. Will need to implement a real one once
        # protocol is further developed.
        data = sock.recv(2048)
        return data


class ChunkServer(BaseServer):
    """
    Base server class for chunkservers. Handles all networking logic that chunkservers use.
    """

    def __init__(self):
        # FIXME: Are the chunkservers on the same port? They should be a different host
        # since they should be running from a seperate machine, but I suppose that shouldn't
        # matter too much.
        super(ChunkServer, self).__init__()
        self.port = config.PORT
        self.host = config.HOST
        self.sock = None
        self.threads = set()

    def initialize_socket(self):
        """
        Initializes and binds a socket for the server on the host and port specified
        in the configuration file.

        :rtype : object
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.host, self.port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            # TODO: LOG and provide means for graceful failure
            print "Unable to open socket: " + message
            print "Error value: " + str(value)


class MasterServer(BaseServer):
    """
    Base server class for the Master class. Handles all networking logic that the Master class
    uses. MasterServer is a multi-threaded TCP server.
    """

    def __init__(self):
        super(MasterServer, self).__init__()
        self._port = config.PORT
        self._host = config.HOST
        self.sock = None
        self.threads = set()
        log.info("INITIALIZED SERVER BASE")

    def initialize_socket(self):
        """
        Initializes and binds a socket for the server on the host and port specified
        in the configuration file.

        :rtype : object
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self._host, self._port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            # TODO: LOG and provide means for graceful failure
            print "Unable to open socket: " + message
            print "Error value: " + str(value)