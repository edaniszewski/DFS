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


class FileInfo:
	""" FileInfo class contains the metadata associated with a file.

	fileName: name of the file
	chunks: list of tuples giving chunkHandle and offset
	delete: flag for Deletion 
	"""
	def __init__(self, fileName):
		self.fileName = fileName
		self.chunks = []
		self.delete = False
		


class ChunkInfo:
	"""ChunkInfo class contains the metadata associated with a chunk.

	chunkHandle: unique ID of the chunk
	chunkserverLocations: list of the chunkserves the chunk exists on
	length: current size of the chunk
	"""
	def __init__(self, chunkHandle, chunkserverLocations=[]):
		self.chunkHandle = chunkHandle
		self.chunkserverLocations = chunkserverLocations
		self.length = 0

	#Return the amount of space already used on the chunk
	def length(self):
		return self.length
		


class GlobalState:
	"""Contains important global state, including the chunkHandle incrementor

	chunkHandle: unique ID of the chunks
	toDelete: list to hold filenames of files flagged for deletion. This increases
	the memory usage, but decreases the overhead of periodically searching for files
	flagged for deletion. Since deletion is not typically a frequent action, the memory
	overhead may be low. This still needs testing to verify.
	"""
	def __init__ (self):
		self.chunkHandle = 0
		self.toDelete = []

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



