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
        Initialize a socket instance and begin listening on the socket
        '''
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
            self.sock.listen(10)
        except socket.error, (value, message):
            if self.sock:
                self.sock.close()
            log.warn("Unable to open socket: " + message)
            log.warn("Error value: " + value)
    
    def run(self):
        '''
        Runs the server instance. Each incoming request is passed off to 
        a thread which receives the data and initiates appropriate action.
        '''
        self.initialize_socket()
        while True:
            t = MasterRequestThread(self.sock.accept())
            t.start()
            self.threads.add(t)
            
        self.sock.close()
        for t in self.threads:
            t.join()

        
class MasterRequestThread(threading.Thread):
    '''
    A class inheriting from the Thread class, overrides the run method 
    to receive and handle incoming data to the master.
    '''
    def __init__(self, (client, address)):
        threading.Thread.__init__(self)
        self.client = client
        self.address = address
        self.size = 1024
    
    # FIXME: Currently, the method contains 'filler' logic
    def run(self):
        running = True
        while running:
            data = self.client.recv(self.size)
            if data:
                self.client.send(data + "\t" + threading.current_thread().name)
            else:
                self.client.close()
                running = False

   
# FIXME: Depricated class
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    '''
    A TCP Server class with multi-threading capabilities
    '''
    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        self.allow_reuse_address=True

# FIXME: Depricated class
class ThreadedTCPHandler(SocketServer.BaseRequestHandler):
    '''
    A threading-enabled TCP request handler
    '''
    def handle(self):
        self.data = self.request.recv(1024)
        current_thread = threading.current_thread()
        response = "%s: Recieved some data!" %(current_thread.name)
        self.request.sendall(response)
        