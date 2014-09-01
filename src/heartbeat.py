'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
import socket
import threading
import time
from src import config
import net


class HeartbeatDict(dict):
    '''
    A dictionary class used to track and manage the active system servers,
    with thread locking.
    '''
    
    def __init__(self):
        '''
        Constructor
        '''
        super(HeartbeatDict, self).__init__()
        self._rwLock = threading.Lock()
        
    # https://docs.python.org/release/2.5.2/ref/sequence-types.html
    def __setitem__(self, key, value):
        '''
        Add an item to the dictionary, or update an entry if it already exists
        '''
        self._rwLock.acquire()
        super(HeartbeatDict, self).__setitem__(key, value)
        self._rwLock.release()
        
    def getStaleEntries(self):
        '''
        Returns a list of dictionary entries that have a time stamp older than
        a threshold amount.
        '''
        #A time limit. Anything less than the time limit is stale, anything greater is still fresh
        staleTime = time.time() - config.heartbeatFreshPeriod
        staleEntries = []
        self._rwLock.acquire()
        #TODO: This isn't awful, but could probably be tightened up a little bit.
        for (ip, time) in self.items():
            if time < staleTime:
                staleEntries.append(ip)
        self._rwLock.release()
        return staleEntries
        
        
        
class HeartbeatListener(threading.Thread):
    '''
    Listen for heart beat pings from HeartbeatClient classes, which are to be instantiated
    with the chunkservers. Update the HeartbeatDict in accordance with the pings received.
    '''
    
    def __init__(self, goOnEvent):
        self.goOnEvent = goOnEvent
        self.hbdict = HeartbeatDict()
        self.sock = net.UDP.getNewUDPSocketConnection();
        
    def run(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(8)
                if data == "<3":
                    self.hbdict[addr[0]] = time.time()
            except socket.timeout:
                pass
            
            
def main():
    event = threading.Event().set()
    listener = HeartbeatListener(event)
    listener.start()


if __name__ == '__main__':
    main()
        
        
        
        
        
        
        
        
        
        
        