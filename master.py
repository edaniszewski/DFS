######################################################
#
#	Erick Daniszewski
#	14 July 2014
#	master.py
#
#	Manage all metadata and metadata requests for 
#	chunks and files.
#
#	--------------------------------------------------
#	History:
#	--------------------------------------------------
#	7.14.2014 - Add outlines for all classes
#
######################################################

import config



class File:
	""" FileInfo class contains the metadata associated with a file.

	fileName: name of the file
	chunks: list of tuples giving chunkHandle and offset
	delete: flag for Deletion 
	size: the size of the file
	"""
	def __init__(self, fileName):
		self.fileName = fileName
		self.chunks = []
		self.delete = False
		self.size = None
		# QUESTION: Do we want to store a chunk-chunkoffset mapping in here?
		


class Chunk:
	"""ChunkInfo class contains the metadata associated with a chunk.

	chunkHandle: unique ID of the chunk
	chunkserverLocations: list of the chunkserves the chunk exists on
	length: current size of the chunk
	"""
	def __init__(self, chunkHandle, chunkserverLocations=[]):
		self.chunkHandle = chunkHandle
		self.chunkserverLocations = chunkserverLocations
		# This is the chunk offset
		self.length = 0

	#Return the amount of space already used on the chunk
	def offset(self):
		return self.length
		


class GlobalState:
	"""Contains important global state, including the chunkHandle incrementor

	chunkHandle: unique ID of the chunks
	toDelete: list to hold filenames of files flagged for deletion. This increases
	the memory usage, but decreases the overhead of periodically searching for files
	flagged for deletion. Since deletion is not typically a frequent action, the memory
	overhead may be low. This still needs testing to verify.
	fileMap: key=filename value=file object
	chunkMap: key=chunkid value=[filename, filename, ...]
	"""
	def __init__(self):
		self.chunkHandle = 0
		self.toDelete = []
		self.fileMap = {}
		self.chunkMap = {}

	#Increments the chunkHandle
	def incrementChunkHandle(self):
		self.chunkHandle += 1

	#Increment and get the chunk handle
	def incrementAndGetChunkHandle(self):
		self.incrementChunkHandle()
		return self.chunkHandle

	#Add a file to the tracked list of pending deletions
	def queueDelete(self, fileName):
		try:
			self.toDelete.append(fileName)
			return 1
		except Exception:
			return 0

	#Remove a file from the tracked list of pending deletions
	def dequeueDelete(self, fileName):
		try:
			self.toDelete.remove(fileName)
			return 1
		except Exception:
			return 0



class Master:
	"""
	Master
	"""
	__init__(self):
		# For now, just make a new global state obj. Later, will need to see if 
		# one already existed, eg, if an oplog can reconstruct the state
		self.globalState = GlobalState()
		# Stores the latest chunk object, since any previous ones will be full.
		self.currentChunk = Chunk(self.globalState.incrementChunkHandle())



	def createFile(self, filename):
		# Check to see if the filename already exists
		if self.globalState.fileMap[filename]:
			print "FILE NAME ALREADY EXISTS"
			return

		# If we got here, the filename does not already exist so, 
		# create a new file object to store its metadata and
		# Add it to the dict of existing files
		self.globalState.fileMap[filename] = File(filename)



	def getUniqueChunkHandle(self):
		self.globalState.incrementChunkHandle()
		return self.globalState.chunkHandle - 1



	def createNewChunk(self):
		self.currentChunk = Chunk(self.getUniqueChunkHandle())
		return 1



	# see current chunk offset if full new chunk, else append until full
	def linkChunkToFile(self, filename, chunkid):
		try:
			fileMeta = self.globalState.fileMap[filename]
			fileMeta.chunks.append(chunkid)

		# If the file does not exist in the dictionary
		except:
			print "FILE NOT FOUND"
			return 



	# Returns a list of files currently found in the system
	def listOfFiles(self):
		return self.globalState.fileMap.keys()


	# When a file is deleted and scrubbed, remove metadata for that file
	def cleanFileMap(self, fileName):
		# Don't know if will need this, but get the associated chunks so they can be
		# unlinked from the file if they need to be elsewhere.
		assicoatedChunks = self.globalState.fileMap[filename].chunks

		try:
			self.globalState.toDelete.remove(filename)
		except:
			print "file not in todelete list......"
			return

		del self.globalState.fileMap[filename]


	def append(self, filename, append_length):

		fileObj = self.globalState.fileMap[fileName]

		chunks = fileObj.chunks

		# Only look at the first replica right now, since all replicas should be the same.
		chunkObj = self.globalState.chunkMap[chunks[0]]

		if ((config.chunkSize - chunkObj.length) < append_length):
			# The size to append is greater than the size left in the chunk.
			pass




