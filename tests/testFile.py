"""
Created on Aug 14, 2014

@author: erickdaniszewski
"""
import unittest

from src.file import File

class Test(unittest.TestCase):

    def setUp(self):
        self.f = File("test_file")

    def tearDown(self):
        pass

    def test_set_password(self):
        self.assertIsNone(self.f._password)
        self.f.set_password("password")
        self.assertIsNotNone(self.f._password)
        self.assertIsInstance(self.f._password, long)

    def test_check_password(self):
        self.f.set_password("password")
        self.assertTrue(self.f.check_password("password"))
        self.assertFalse(self.f.check_password("not_password"))

        self.f.set_password(12345)
        self.assertTrue(self.f.check_password(12345))
        self.assertFalse(self.f.check_password(54321))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()