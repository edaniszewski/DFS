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
		# Get all needed configured variables
		self.port = config.port
		self.timeout = config.sock_timeout
		self.delay = config.delay
		self.hosts = config.hostsfile
		self.activehosts = config.activehostsfile
		# Get an initial list of all chunkserver IPs. This could suck
		# for a massive list, but its only run once...
		self.chunkserverIPs = self.getChunkserverIPs()
		# Do the same for active IPs. 
		self.activeChunkserverIPs = self.getActiveChunkserverIPs()
		# Queue list of IPs to add to active chunkservers.
		# having this as single list makes it so the activehosts.txt
		# file need not be opened and closed multiple times, only once.
		self.queuedactive = []
		# Queue of IPs to be removed from active chunkservers
		self.queuedinactive = []


	def getChunkserverIPs(self):
		try:
			# Forward declare an array to store ips
			ips = []
			# Read in lines from file as a stream
			with open(self.hosts, 'r') as f:
				for line in f:
					ips.append(line)
			# Return the list of all chunkserverIPS
			return ips
		# If the file does not exist, alert the user
		# TODO: This error needs to be logged, not printed out.
		except IOError as e:
			print "ERROR: HOSTS FILE NOT FOUND"


	def getActiveChunkserverIPs(self):
		try:
			# Forward declare an array to store ips
			ips = []
			# Read in lines from file as a stream
			with open(self.hosts, 'r') as f:
				for line in f:
					ips.append(line)
			# Return the list of active chunkserver IPs
			return ips
		# If the file does not exist, alert the user
		# TODO: This error needs to be logged, not printed out.
		except IOError as e:
			print "ERROR: AHOSTS FILE NOT FOUND. NEW FILE CREATED."
			# Create an ahosts.txt -- kind of a hack, but it works for now.
			open(self.AHOSTS, "a").close()
			# Since there is nothing in the new file, return nothing
			return []



			##################################
			# OUTLINE
			##################################


	def pulse(self):
		try:
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.s.settimeout(self.timeout)
			self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			self.s.connect((IP, self.port))
			self.s.send('SOME MESSAGE HERE')
			response = self.s.recv()
				
			if response == "SOME RESPONSE HERE":
				if IP not in self.activeChunkserverIPs:
					self.queuedactive.append(IP)
			else:
				# This would need to be logged, not printed.
				print "You got back a response you shouldn't have.. reporting you!"

		except (socket.timeout, socket.error):
			if IP in self.activeChunkserverIPs:
				self.queuedinactive.append(IP)


