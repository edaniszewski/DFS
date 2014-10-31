'''
A message class to hold system network message constants. Using byte-sized
constants as communication between various parts of the system results in 
a reduced network load.

Created on Oct 31, 2014

@author: erickdaniszewski
'''


class Message:
    READ = 1
    WRITE = 2
    APPEND = 3
    DELETE = 4
    UNDELETE = 5
    SANITIZE = 6
    GETITEMSTODELETE = 7
    GETALLCHUNKS = 8
    GETLOCATIONS = 9
    GETFILENAMES = 10