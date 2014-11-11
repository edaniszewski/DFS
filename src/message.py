"""
A message class to hold system network message constants. Using byte-sized
constants as communication between various parts of the system results in
a reduced network load.

Created on Oct 31, 2014

@author: erickdaniszewski
"""


class Message(object):
    """
    Message codes for intra-system communications
    """

    def __init__(self):
        self.FAILURE = 0
        self.SUCCESS = 1
        self.READ = 2
        self.APPEND = 3
        self.DELETE = 4
        self.UNDELETE = 5
        self.SANITIZE = 6
        self.CREATE = 7
        self.OPEN = 8
        self.CLOSE = 9
        self.WRITE = 10
        self.CONTENTS = 11
        self.CHUNKSPACE = 12
        self.HEARTBEAT = 13
