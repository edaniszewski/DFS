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
chunk_size = 2 ** 10
replica_amount = 3
PORT = 9500  # TODO: look in to which port would be best, if it even matters
HOST = '127.0.0.1'
heartbeat_fresh_period = 15
heartbeat_timeout = 10
heartbeat_port = 9550
heartbeat_host = '127.0.0.1'
beat_period = 5