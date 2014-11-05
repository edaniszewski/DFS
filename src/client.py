"""
Created on Aug 13, 2014

@author: erickdaniszewski
"""
import socket

import config


class Client(object):
    """
    User-end client
    """

    def __init__(self):
        self.BUF_SIZE = 1024

    def client(self, ip, port, message):
        # Connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        try:
            sock.sendall(message)
            response = sock.recv(self.BUF_SIZE)
            print "Client received: %s" % response
        finally:
            sock.close()


if __name__ == '__main__':
    client = Client()

    while 1:
        client.client(config.HOST, config.PORT, raw_input(">"))