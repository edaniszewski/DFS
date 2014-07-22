######################################################
#
#	Erick Daniszewski
#	14 July 2014
#	heartbeat.py
#
#	Periodically pings chunkservers to maintain a 
#	current view of the state of the system.
#
#	--------------------------------------------------
#	History:
#	--------------------------------------------------
#
######################################################

import config

class heartBeat:
	"""
	The heartbeat periodically pings chunkservers to check their 
	status. If a chunkserver is unresponsive, the list of active 
	chunkservers is updated accordingly.
	"""
	def __init__(self):
		self.port = config.port
		self.timeout = config.sock_timeout
		self.delay = config.delay
		hosts = config.hostsfile
		activehosts = config.activehostsfile
