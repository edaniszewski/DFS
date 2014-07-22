######################################################
#
#	Erick Daniszewski
#	18 July 2014
#	net.py
#
#	Network and socket logic
#
#	--------------------------------------------------
#	History:
#	--------------------------------------------------
#
######################################################


import socket


def createSocket():
	# Create TCP socket object
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# Allow address reuse
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	# Return the new socket connection
	return s


def socketConnect(socket, address, port):
	try:
		# Try establishing a connection
		socket.connect((address, port))
		# Return the socket object
		return socket
	except:
		# TODO : Improve error handling. 
		return None

