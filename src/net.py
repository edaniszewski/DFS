'''
The net module defines the base classes for the networking capability
of the master server, the chunk server, and other network-related methods 
used by the system.

Created on Aug 13, 2014

@author: erickdaniszewski
'''
import socket
import config
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("net_logger")

class TCP:
    '''
    A TCP socket networking object
    '''
    
    def get_new_tcp_socket_connection(self, host, port):
        '''
        Create a new socket object, connect to the given host on the given port
        and return the socket object
        '''
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.s.connect((host, port))
            return self.s
        except socket.error, (value,message):
            if self.s: 
                self.s.close() 
            log.warn("Could not open socket: " + message)
        
        
    def send(self, sock, data):
        '''
        Send data over a given socket
        '''
        sock.sendall(data)
        
        
    def get_new_socket_and_send(self, host, port, data):
        '''
        Create a new socket and send data over the socket
        '''
        self.send(self.get_new_tcp_socket_connection(host, port), data)
        
        
    def receive(self, sock):
        '''
        Receive data from a socket connection
        '''
        #TODO: This is just a dummy receive for now. Will need to implement a real one once protocol is further developed.
        data = sock.recv(2048)
        return data
        
        
        
class UDP:
    '''
    A UDP socket networking object
    '''
    
    def get_new_udp_socket_connection(self):
        '''
        Create a new socket object, connect to the given host on the given port
        and return the socket object
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(config.heartbeatTimeout)
        # TODO: this seems weird, need to think about how to define ports/host for heartbeat udp
        s.bind((config.heartbeatHost, config.heartbeatPort))
        return s
        
        
    def send(self, sock, data):
        '''
        Send data over a given socket
        '''
        sock.sendto(data, (config.heartbeatHost, config.heartbeatPort))
        
        
    def get_new_socket_and_send(self, host, port, data):
        '''
        Create a new socket and send data over the socket
        '''
        self.send(self.get_new_udp_socket_connection(host, port), data)
        
        
    def receive(self, sock):
        '''
        Receive data from a socket connection
        '''
        #TODO: This is just a dummy receive for now. Will need to implement a real one once protocol is further developed.
        data = sock.recv(2048)
        return data


   
class MasterServer():
    '''
    Base server class for the Master class. Handles all networking logic that the Master class
    uses. MasterServer is a multi-threaded TCP server.
    '''
    def __init__(self):
        self.port = config.PORT
        self.host = config.HOST
        self.sock = None
        self.threads = set()
        
    def initialize_socket(self):
        '''
        Initializes and binds a socket for the server on the host and port specified
        in the configuration file.
        '''
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            #TODO: LOG and provide means for graceful failure
            print "Unable to open socket: " + message
            print "Error value: " + str(value)

    
