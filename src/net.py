'''
The net module defines the base classes for the networking capability
of the master server, the chunk server, and other network-related methods 
used by the system.

Created on Aug 13, 2014

@author: erickdaniszewski
'''
import SocketServer
import threading
import socket
from src import config

class TCP:
    '''
    A TCP socket networking object
    '''
    
    def getNewTCPSocketConnection(self, host, port):
        '''
        Create a new socket object, connect to the given host on the given port
        and return the socket object
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((host, port))
        return s
        
        
    def send(self, sock, data):
        '''
        Send data over a given socket
        '''
        sock.sendall(data)
        
        
    def getNewSocketAndSend(self, host, port, data):
        '''
        Create a new socket and send data over the socket
        '''
        self.send(self.getNewTCPSocketConnection(host, port), data)
        
        
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
    
    def getNewUDPSocketConnection(self):
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
        
        
    def getNewSocketAndSend(self, host, port, data):
        '''
        Create a new socket and send data over the socket
        '''
        self.send(self.getNewUDPSocketConnection(host, port), data)
        
        
    def receive(self, sock):
        '''
        Receive data from a socket connection
        '''
        #TODO: This is just a dummy receive for now. Will need to implement a real one once protocol is further developed.
        data = sock.recv(2048)
        return data

    
    
        

        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    '''
    A TCP Server class with multi-threading capabilities
    '''
    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        self.allow_reuse_address=True


class ThreadedTCPHandler(SocketServer.BaseRequestHandler):
    '''
    A threading-enabled TCP request handler
    '''
    def handle(self):
        # TODO: Implement stronger send/recv - potentially using pickle 
        self.data = self.request.recv(1024)
        current_thread = threading.current_thread()
        response = "%s: Recieved some data!" %(current_thread.name)
        self.request.sendall(response)
        