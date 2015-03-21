"""
The File class is an object used to track and maintain the metadata
associated with files by the system. The Global State, which is
instantiated by the Master, contains associations between the files
and the chunks which make them up.

Notable components of a file File are:

1. File name:
    The name which identifies the file.

2. Associated chunk handles:
    The identifiers for the chunks which contain the data for the
    file.


###############################################################################
The MIT License (MIT)

Copyright (c) 2014 Erick Daniszewski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
###############################################################################
"""
import hashlib


class File(object):
    """
    Contains the metadata associated with a file.
    """

    def __init__(self, file_name):
        self.file_name = file_name
        self.chunk_handles = []
        self.delete = False
        self.size = None
        self.namespace = None
        self._password = None

    def set_password(self, password):
        """
        Set the password for the file

        :param password: The password to set for the file
        """
        self._password = int(hashlib.md5("{}:{}".format(self.file_name, password)).hexdigest(), 16)

    def check_password(self, password):
        """
        Validate that a given password matches the password set for the file

        :param password: The password to check
        :rtype bool
        :return: True if the password matches, False otherwise.
        """
        return self._password == int(hashlib.md5("{}:{}".format(self.file_name, password)).hexdigest(), 16)