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
        self.READ = 1
        self.APPEND = 2
        self.DELETE = 3
        self.UNDELETE = 4
        self.SANITIZE = 5
        self.CREATE = 6
        self.OPEN = 7
        self.CLOSE = 8
        self.WRITE = 9
        self.CONTENTS = 10
        self.CHUNKSPACE = 11
