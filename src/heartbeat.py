"""
The heartbeat is a means of detecting and tracking the servers that make up the
system. When a server joins the system, it will ping the heartbeat listener, which
adds the server to a dictionary which takes the server ip as key and the time at which
the ping was received as the value. Correctly operating servers are expected to ping
the heartbeat listener periodically, so a lapse in response over some threshold value
will mark that ip as inactive.

The server states tracked by the heartbeat module are used by the Master to delegate
chunks and files to servers and maintaining a minimum number of replications on active servers.

Created on Aug 13, 2014

@author: erickdaniszewski
"""
import socket
import threading
import time
import logging

import config
from net import UDP


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("heartbeat_logger")


# TODO: Need a way to persist this dict?
class HeartbeatDict(dict):
    """
    A dictionary class used to track and manage the active system servers,
    with thread locking.
    """

    def __init__(self):
        """
        Constructor initializes the dictionary and creates a thread lock
        """
        super(HeartbeatDict, self).__init__()
        self._rwLock = threading.Lock()
        self.activeHosts = []

    # https://docs.python.org/release/2.5.2/ref/sequence-types.html
    def __setitem__(self, key, value):
        """
        Add an item to the dictionary, or update an entry if it already exists.
        """
        with self._rwLock:
            super(HeartbeatDict, self).__setitem__(key, value)

    def get_entries(self):
        """
        Returns a two-tuple of lists of dictionary entries. The 0th element in the
        tuple is a list of all active IPs. The 1st element is a list of IPs that have a
        time stamp older than a threshold amount (inactive IPs).
        """
        #A time limit. Anything less than the time limit is stale, anything greater is still fresh
        stale_time = time.time() - config.heartbeat_fresh_period
        stale_entries, fresh_entries = set(), set()
        with self._rwLock:
            #TODO: This isn't awful, but could probably be tightened up a little bit.
            for (ip, ping_time) in self.items():
                stale_entries.add(ip) if ping_time < stale_time else fresh_entries.add(ip)

                # Code below could be more optimized for the code above..
                # ([staleEntries.add(ip) for (ip, time) in self.items() if time < staleTime else freshEntries.add(ip)])
        return fresh_entries, stale_entries

    def update_active_hosts(self):
        """
        Update the list of active hosts within the system. First, remove any hosts that are stale,
        then add any new hosts to activehosts.
        """
        results = self.get_entries()
        self.activeHosts.remove([stale_entry for stale_entry in results[1] if stale_entry in self.activeHosts])
        self.activeHosts = set(self.activeHosts.extend(results[0]))


class HeartbeatListener(threading.Thread):
    """
    Listen for heart beat pings from HeartbeatClient classes, which are to be instantiated
    with the chunkservers. Update the HeartbeatDict in accordance with the pings received.
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.hbdict = HeartbeatDict()
        self.sock = None

    def initialize_socket(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(config.heartbeat_timeout)
            s.bind((config.heartbeat_host, config.heartbeat_port))
            self.sock = s
        except socket.error:
            if self.sock:
                self.sock.close()
            log.error("Could not initialize heartbeat listener socket")

    def run(self):
        self.initialize_socket()

        while True:
            try:
                data, addr = self.sock.recvfrom(8)
                if data == "<3":
                    self.hbdict[addr[0]] = time.time()
            except socket.timeout:
                pass


class HeartbeatClient(UDP):
    """
    A class each chunkserver will instantiate in order to send heartbeat messages to the
    HeartbeatListener
    """

    def __init__(self):
        UDP.__init__(self)
        self.msg = "<3"

    def ping(self):
        """
        Ping the heartbeat listener
        """
        self.send(self.socket, self.msg)

    def ping_forever(self):
        """
        Pings out to the heartbeat listener forever
        """
        while True:
            self.ping()
            time.sleep(config.beat_period)


def main():
    listener = HeartbeatListener()
    listener.start()


if __name__ == '__main__':
    main()