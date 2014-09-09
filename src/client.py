'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
import socket
import config

class Client(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.BUF_SIZE = 1024
        
        
    def client(self, ip, port, message):
        # Connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        try:
            sock.sendall(message)
            response = sock.recv(self.BUF_SIZE)
            print "Client received: %s" %response
        finally:
            sock.close()
            
            
if __name__=="__main__":
    client = Client()
    
    for x in range(10):
        client.client('127.0.0.1', config.PORT, "message " + str(x))