"""
Created on Aug 14, 2014

@author: erickdaniszewski
"""
import unittest
from src.globalstate import GlobalState
from src.file import File
from src.chunk import Chunk


class Test(unittest.TestCase):

    def setUp(self):
        self.gs = GlobalState()
        self.fileName = "test_file_1"

    def tearDown(self):
        pass

    def testIncrementChunkHandle(self):        
        self.assertEqual(0, self.gs.chunkHandle, "initial chunk handle")
        self.assertEqual(1, self.gs.increment_and_get_chunk_handle(), "incremented chunk handle")

    def testAddFile(self):
        self.assertEqual(0, len(self.gs.file_map), "check that filemap is initially empty")
        self.assertTrue(self.gs.add_file(self.fileName), "check for proper return code")
        self.assertEqual(1, len(self.gs.file_map), "check that single entry exists")
        self.assertFalse(self.gs.add_file(self.fileName), "should not re-create existing file")
        self.assertEqual(1, len(self.gs.file_map), "check that single entry exists")

    def testQueueingDelete(self):
        self.gs.add_file(self.fileName)
        
        self.assertEqual(0, len(self.gs.to_delete), "check that filemap is initially empty")
        self.assertTrue(self.gs.queue_delete(self.fileName), "check for proper return code")
        self.assertEqual(self.fileName, self.gs.to_delete[0], "check that proper file name is kept")
        self.assertEqual(1, len(self.gs.to_delete), "check that single entry exists")
        self.assertFalse(self.gs.queue_delete(self.fileName + "1"), "check handling of invalid file name")
        self.assertTrue(self.gs.dequeueDelete(self.fileName), "check for proper return code")
        self.assertEqual(0, len(self.gs.to_delete), "check that filemap is empty")
        self.assertFalse(self.gs.dequeueDelete(self.fileName), "test removing something that is not in the list")

    def testGetFile(self):
        self.gs.add_file(self.fileName)
        self.assertIsInstance(self.gs.get_file(self.fileName), File)
        self.assertFalse(self.gs.get_file("null"))

    def testGetFiles(self):
        self.gs.add_file(self.fileName + "1")
        self.gs.add_file(self.fileName + "2")
        self.gs.add_file(self.fileName + "3")
        
        files = self.gs.get_files()
        
        self.assertEqual(3, len(files))
        
        for obj in files:
            self.assertIsInstance(obj, File)

    def testGetFileNames(self):
        self.gs.add_file(self.fileName + "1")
        self.gs.add_file(self.fileName + "2")
        self.gs.add_file(self.fileName + "3")
        
        names = self.gs.get_file_names()
        
        self.assertEqual(3, len(names))
        
        for name in names:
            self.assertIsInstance(name, str)

    def testCleanFileMap(self):
        self.gs.add_file(self.fileName + "1")
        self.gs.add_file(self.fileName + "2")
        self.gs.add_file(self.fileName + "3")
        
        self.assertEqual(3, len(self.gs.file_map))
        
        self.gs.clean_file_map(self.fileName + "2")
        
        self.assertEqual(2, len(self.gs.file_map))
        names = self.gs.get_file_names()
        self.assertNotIn(self.fileName + "2", names)

    def testAddChunk(self):
        self.assertEqual(0, len(self.gs.chunk_map), "check that chunkmap is initially empty")
        self.assertTrue(self.gs.add_chunk(1), "check for proper return code")
        self.assertEqual(1, len(self.gs.chunk_map), "check that single entry exists")
        self.assertFalse(self.gs.add_chunk(1), "should not re-create existing chunk")
        self.assertEqual(1, len(self.gs.chunk_map), "check that single entry exists")

    def testGetChunk(self):
        self.gs.add_chunk(1)
        self.assertIsInstance(self.gs.get_chunk(1), Chunk)
        self.assertFalse(self.gs.get_chunk(10))
        
    def testGetChunks(self):
        self.gs.add_chunk(1)
        self.gs.add_chunk(2)
        self.gs.add_chunk(3)
        
        chunks = self.gs.get_chunks()
        
        self.assertEqual(3, len(chunks))
        
        for obj in chunks:
            self.assertIsInstance(obj, Chunk)

    def testGetChunkIDs(self):
        self.gs.add_chunk(1)
        self.gs.add_chunk(2)
        self.gs.add_chunk(3)
        
        chunkHandles = self.gs.get_chunk_ids()
        
        self.assertEqual(3, len(chunkHandles))
        
        for chunkHandle in chunkHandles:
            self.assertIsInstance(chunkHandle, int)

    def testCleanChunkMap(self):
        self.gs.add_chunk(1)
        self.gs.add_chunk(2)
        self.gs.add_chunk(3)
        
        self.assertEqual(3, len(self.gs.chunk_map))
        
        self.gs.clean_chunk_map(2)
        
        self.assertEqual(2, len(self.gs.chunk_map))
        chunkHandles = self.gs.get_chunk_ids()
        self.assertNotIn(2, chunkHandles)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()