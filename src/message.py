"""
A message class to hold system network message constants. Using byte-sized
constants as communication between various parts of the system results in
a reduced network load.

Created on Oct 31, 2014

@author: erickdaniszewski
"""


class Message:
    """
    Byte-size message codes for system wide communications
    """

    def __init__(self):
        self.SUCCESS = 0
        self.FAILURE = 1
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
