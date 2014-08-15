'''
Created on Aug 14, 2014

@author: erickdaniszewski
'''
import unittest
from src.meta.globalstate import GlobalState



class Test(unittest.TestCase):


    def setUp(self):
        self.gs = GlobalState()
        self.fileName = "test_file_1"


    def tearDown(self):
        pass


    def testIncrementChunkHandle(self):        
        self.assertEqual(0, self.gs.chunkHandle, "initial chunk handle")
        self.assertEqual(1, self.gs.incrementAndGetChunkHandle(), "incremented chunk handle")
        
        
    def testAddFile(self):
        self.assertEqual(0, len(self.gs.fileMap), "check that filemap is initially empty")
        self.assertTrue(self.gs.addFile(self.fileName), "check for proper return code")
        self.assertEqual(1, len(self.gs.fileMap), "check that single entry exists")
        self.assertFalse(self.gs.addFile(self.fileName), "should not re-create existing file")
        self.assertEqual(1, len(self.gs.fileMap), "check that single entry exists")
        
        
    def testQueueingDelete(self):
        self.assertEqual(0, len(self.gs.toDelete), "check that filemap is initially empty")
        self.assertTrue(self.gs.queueDelete(self.fileName), "check for proper return code")
        self.assertEqual(self.fileName, self.gs.toDelete[0], "check that proper file name is kept")
        self.assertEqual(1, len(self.gs.toDelete), "check that single entry exists")
        self.assertTrue(self.gs.dequeueDelete(self.fileName), "check for proper return code")
        self.assertEqual(0, len(self.gs.toDelete), "check that filemap is empty")
        self.assertFalse(self.gs.dequeueDelete(self.fileName), "test removing something that is not in the list")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()