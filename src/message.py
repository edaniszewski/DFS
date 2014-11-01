'''
A message class to hold system network message constants. Using byte-sized
constants as communication between various parts of the system results in 
a reduced network load.

Created on Oct 31, 2014

@author: erickdaniszewski
'''


class Message:
    READ = 1
    APPEND = 2
    DELETE = 3
    UNDELETE = 4
    SANITIZE = 5
