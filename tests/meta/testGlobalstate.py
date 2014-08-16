'''
Created on Aug 14, 2014

@author: erickdaniszewski
'''
import unittest
from src.meta.globalstate import GlobalState
from src.meta.file import File
from src.meta.chunk import Chunk




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
        self.gs.addFile(self.fileName)
        
        self.assertEqual(0, len(self.gs.toDelete), "check that filemap is initially empty")
        self.assertTrue(self.gs.queueDelete(self.fileName), "check for proper return code")
        self.assertEqual(self.fileName, self.gs.toDelete[0], "check that proper file name is kept")
        self.assertEqual(1, len(self.gs.toDelete), "check that single entry exists")
        self.assertFalse(self.gs.queueDelete(self.fileName + "1"), "check handling of invalid file name")
        self.assertTrue(self.gs.dequeueDelete(self.fileName), "check for proper return code")
        self.assertEqual(0, len(self.gs.toDelete), "check that filemap is empty")
        self.assertFalse(self.gs.dequeueDelete(self.fileName), "test removing something that is not in the list")


    def testGetFile(self):
        self.gs.addFile(self.fileName);
        self.assertIsInstance(self.gs.getFile(self.fileName), File)
        self.assertFalse(self.gs.getFile("null"))
        
        
    def testGetFiles(self):
        self.gs.addFile(self.fileName + "1")
        self.gs.addFile(self.fileName + "2")
        self.gs.addFile(self.fileName + "3")
        
        files = self.gs.getFiles()
        
        self.assertEqual(3, len(files))
        
        for obj in files:
            self.assertIsInstance(obj, File)
         
            
    def testGetFileNames(self):
        self.gs.addFile(self.fileName + "1")
        self.gs.addFile(self.fileName + "2")
        self.gs.addFile(self.fileName + "3")
        
        names = self.gs.getFileNames()
        
        self.assertEqual(3, len(names))
        
        for name in names:
            self.assertIsInstance(name, str)
        
        
    def testCleanFileMap(self):
        self.gs.addFile(self.fileName + "1")
        self.gs.addFile(self.fileName + "2")
        self.gs.addFile(self.fileName + "3")
        
        self.assertEqual(3, len(self.gs.fileMap))
        
        self.gs.cleanFileMap(self.fileName + "2")
        
        self.assertEqual(2, len(self.gs.fileMap))
        names = self.gs.getFileNames()
        self.assertNotIn(self.fileName + "2", names)
        
        
    def testAddChunk(self):
        self.assertEqual(0, len(self.gs.chunkMap), "check that chunkmap is initially empty")
        self.assertTrue(self.gs.addChunk(1), "check for proper return code")
        self.assertEqual(1, len(self.gs.chunkMap), "check that single entry exists")
        self.assertFalse(self.gs.addChunk(1), "should not re-create existing chunk")
        self.assertEqual(1, len(self.gs.chunkMap), "check that single entry exists")
        
        
    def testGetChunk(self):
        self.gs.addChunk(1);
        self.assertIsInstance(self.gs.getChunk(1), Chunk)
        self.assertFalse(self.gs.getChunk(10))
        
    def testGetChunks(self):
        self.gs.addChunk(1)
        self.gs.addChunk(2)
        self.gs.addChunk(3)
        
        chunks = self.gs.getChunks()
        
        self.assertEqual(3, len(chunks))
        
        for obj in chunks:
            self.assertIsInstance(obj, Chunk)
        
        
    def testGetChunkIDs(self):
        self.gs.addChunk(1)
        self.gs.addChunk(2)
        self.gs.addChunk(3)
        
        chunkHandles = self.gs.getChunkIDs()
        
        self.assertEqual(3, len(chunkHandles))
        
        for chunkHandle in chunkHandles:
            self.assertIsInstance(chunkHandle, int)
        
        
    def testCleanChunkMap(self):
        self.gs.addChunk(1)
        self.gs.addChunk(2)
        self.gs.addChunk(3)
        
        self.assertEqual(3, len(self.gs.chunkMap))
        
        self.gs.cleanChunkMap(2)
        
        self.assertEqual(2, len(self.gs.chunkMap))
        chunkHandles = self.gs.getChunkIDs()
        self.assertNotIn(2, chunkHandles)
        
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()