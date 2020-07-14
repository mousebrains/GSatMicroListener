#
# Read a message from a socket and send the message
# a set of queues for processing
#
# Feb-2020, Pat Welch, pat@mousebrains.com

from datetime import datetime, timezone
import threading
import socket
import queue
import argparse
import logging

class Reader(threading.Thread):
    ''' Read from a connection, parse it, and send to the output queue '''
    def __init__(self, conn, addr, logger:logging.Logger, q:list):
        threading.Thread.__init__(self, daemon=True)
        self.name = 'Reader({}:{})'.format(addr[0], addr[1])
        self.conn = conn
        self.addr = addr
        self.logger = logger
        self.q = q

    def run(self) -> None:
        '''Called on thread start '''
        try:
            msg = b''
            with self.conn as conn:
                t0 = datetime.now(tz=timezone.utc)
                while True: # Get the whole message until the socket is closed
                    data = conn.recv(8192) # Get the data
                    self.logger.debug('data=%s', data)
                    if not data: break # connection has dropped
                    msg += data
            vals = (t0, self.addr, msg)
            for q in self.q:
                q.put(vals)
        except:
            self.logger.exception('Exception while reading from address %s', self.addr)
