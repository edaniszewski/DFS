######################################################
#
#	Erick Daniszewski
#	14 July 2014
#	config.py
#
#	Holds all configurable global variables.
#
#	--------------------------------------------------
#	History:
#	--------------------------------------------------
#
######################################################

chunkSize = 2**10 # This is clearly not to spec, but better for testing.
chunkDir = 'chunks'

masterAddr = 'localhost'
port = 6666


sock_timeout = 2
delay = 0.1

activehostsfile = 'activehosts.txt'
hostsfile = 'hosts.txt'