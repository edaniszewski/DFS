'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
import SocketServer
import threading


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    '''
    A TCP Server class with threading capabilities
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
        