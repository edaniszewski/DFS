'''
The File class is a data structure used to track and maintain the 
metadata associated with files within the system.

Created on Aug 13, 2014

@author: erickdaniszewski
'''

class File(object):
    '''
    Contains the metadata associated with a file.
    
    @var fileName: name of the file
    @var chunkHandles: list of chunkHandles of associated chunks
    @var delete: flag for deletion
    @var size: the size of the file
    '''

    def __init__(self, fileName):
        '''
        Constructor
        '''
        #TODO: Would it make sense to hash the filename?
        #TODO: NAMESPACE implementation - could be as simple as adding a namespace field
        #TODO: FUTURE FEATURE: password-enabled files
        self.fileName = fileName
        self.chunkHandles = []
        self.delete = False
        self.size = None
        