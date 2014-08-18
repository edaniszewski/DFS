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
    pass


class ThreadedTCPHandler(SocketServer.BaseRequestHandler):
    '''
    A threading-enabled TCP request handler
    '''
    def handle(self):
        # TODO: Implement stronger send/recv - potentially using pickle 
        data = self.request.recv(1024)
        current_thread = threading.current_thread()
        response = "%s: %s" %(current_thread.name, data)
        self.request.sendall(response)
        