'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''

class File(object):
    '''
    Contains the metadata associated with a file.

    fileName: name of the file
    chunkHandles: list of chunkHandles
    delete: flag for Deletion 
    size: the size of the file
    '''


    def __init__(self, fileName):
        '''
        Constructor
        '''
        self.fileName = fileName
        self.chunkHandles = []
        self.delete = False
        self.size = None
        