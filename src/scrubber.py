"""
The scrubber acts as a garbage collector. The Master tracks deletions and stores
which files have been deleted. The scrubber takes this information and cleans up
the metadata appropriately.

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
import logging
import socket

import config
from message import Message

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scrubber_logger")


class Scrubber(object):
    """
    A garbage collection class. Cleans up the metadata structures periodically for files
    which have been marked for deletion.
    """

    def __init__(self):
        self.m = Message()
        self.to_delete = None
        self.timeout = 3

    def collect_garbage(self, items_to_delete):
        """
        Method to gather items to be deleted from the master.

        :param items_to_delete:
        :return:
        """
        self.to_delete = items_to_delete

    def connect_to_chunkserver(self, addr, retry_count=0):
        """
        Initiates a connection to a specified chunkserver.

        :rtype : object
        :param retry_count:
        :param addr:
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((addr, config.PORT))
            log.info("Successfully connected to chunkserver at " + str(addr))

        except (socket.error, socket.timeout):
            if retry_count < 3:
                log.warn(
                    "Unable to connect to chunkserver to initiate deletion at address: " + str(addr) + "\tRetrying...")
            else:
                log.error("Unable to connect to chunkserver at " + str(addr))

    def clean(self, addr, chunk_handle):
        """
        Send a delete request to a chunkserver for a specified chunk

        :rtype : object
        :param addr:
        :param chunk_handle:
        """
        socket = self.connect_to_chunkserver(addr)

        try:
            # TODO: need to impelement override methods of send/recv to account for sys messages
            socket.send(self.m.SANITIZE)

            state = socket.recv(1024)
            return state

        except (socket.error, socket.timeout):
            log.error("Error connecting with chunkserver. No guarantee of chunk deletion.")
            return self.m.FAILURE

    def sanitize(self):
        """
        Method to handle the cleaning of all the chunks which are marked for deletion

        :rtype : object
        """
        pass