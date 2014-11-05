"""
System configuration options

Created on Aug 16, 2014

@author: erickdaniszewski
"""

## FILE PATH DEFINITIONS
oplog = "../resources/OPLOG.log"
hosts = "../resources/all.hosts"
activehosts = "../resources/active.hosts"
metasnapshot = "../resources/meta.snapshot"
chunkstore = "../chunkstore/"


## SYSTEM CONSTANTS
chunkSize = 2 ** 10
replicaAmount = 3
PORT = 9500  # TODO: look in to which port would be best, if it even matters
HOST = '127.0.0.1'
heartbeatFreshPeriod = 15
heartbeatTimeout = 10
heartbeatPort = 9550
heartbeatHost = '127.0.0.1'
beatPeriod = 5