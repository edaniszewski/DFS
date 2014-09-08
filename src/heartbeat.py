'''
Created on Aug 13, 2014

@author: erickdaniszewski
'''
import socket
import threading
import time
from src import config
import net
from net import UDP


class HeartbeatDict(dict):
    '''
    A dictionary class used to track and manage the active system servers,
    with thread locking.
    '''
    
    def __init__(self):
        '''
        Constructor initializes the dictionary and creates a thread lock
        '''
        super(HeartbeatDict, self).__init__()
        self._rwLock = threading.Lock()
        
    # https://docs.python.org/release/2.5.2/ref/sequence-types.html
    def __setitem__(self, key, value):
        '''
        Add an item to the dictionary, or update an entry if it already exists.
        '''
        with self._rwLock:
            super(HeartbeatDict, self).__setitem__(key, value)
        
    def getEntries(self):
        '''
        Returns a two-tuple of lists of dictionary entries. The 0th element in the
        tuple is a list of all active IPs. The 1st element is a list of IPs that have a 
        time stamp older than a threshold amount (inactive IPs).
        '''
        #A time limit. Anything less than the time limit is stale, anything greater is still fresh
        staleTime = time.time() - config.heartbeatFreshPeriod
        staleEntries = []
        freshEntries = []
        with self._rwLock:
            #TODO: This isn't awful, but could probably be tightened up a little bit.
            for (ip, time) in self.items():
                staleEntries.append(ip) if time < staleTime else freshEntries.append(ip)
        return (freshEntries, staleEntries)
        
        
        
class HeartbeatListener(threading.Thread):
    '''
    Listen for heart beat pings from HeartbeatClient classes, which are to be instantiated
    with the chunkservers. Update the HeartbeatDict in accordance with the pings received.
    '''
    
    def __init__(self, event):
        self.event = event
        self.hbdict = HeartbeatDict()
        self.sock = net.UDP.getNewUDPSocketConnection();
        
    def run(self):
        while self.event.isSet():
            try:
                data, addr = self.sock.recvfrom(8)
                if data == "<3":
                    self.hbdict[addr[0]] = time.time()
            except socket.timeout:
                pass
            
            
class HeartbeatClient():
    '''
    A class each chunkserver will instantiate in order to send heartbeat messages to the 
    HeartbeatListener
    '''
    pass
    
    
            
            
def main():
    event = threading.Event().set()
    listener = HeartbeatListener(event)
    listener.start()


if __name__ == '__main__':
    main()
        
        
        
        
        
        
        
        
        
        
        