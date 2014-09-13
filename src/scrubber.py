'''
The scrubber acts as a garbage collector. The Master tracks deletions and stores
which files have been deleted. The scrubber takes this information and cleans up 
the metadata appropriately.

Created on Aug 13, 2014

@author: erickdaniszewski
'''

class Scrubber(object):
    '''
    A garbage collection class. Cleans up the metadata structures periodically for files
    which have been marked for deletion. 
    '''

    def __init__(self, params):
        '''
        Constructor
        '''
        