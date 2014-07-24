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


class File:
	""" FileInfo class contains the metadata associated with a file.

	fileName: name of the file
	chunks: list of tuples giving chunkHandle and offset
	delete: flag for Deletion 
	"""
	def __init__(self, fileName):
		self.fileName = fileName
		self.chunks = []
		self.delete = False
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

	#Increments the chunkHandle and returns the new chunkHandle value
	def incrementChunkHandle(self):
		self.chunkHandle += 1
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
		# Starts at 1
		return self.globalState.incrementChunkHandle()



	def createNewChunk(self):
		self.currentChunk = Chunk(self.globalState.incrementChunkHandle())




	# see current chunk offset if full new chunk, else append until full
	def linkChunkToFile(self, filename, chunkid):
		try:
			fileMeta = self.globalState.fileMap[filename]
			fileMeta.chunks.append(chunkid)

		# If the file does not exist in the dictionary
		except:
			print "FILE NOT FOUND"
			return 




