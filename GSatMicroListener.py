#! /usr/bin/env python3
#
# Listen to a socket
# When a connection is made,
# spawn a thread, parse the message,
# and send to the writer thread.
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import time
import threading
import socket
import queue
import argparse
import logging
import logging.handlers
import sqlite3
import os.path

def addLoggerArgs(parser:argparse.ArgumentParser) -> None:
    grp = parser.add_argument_group('Logger Related Options')
    grp.add_argument('--logfile', type=str, help='Name of logfile')
    grp.add_argument('--logBytes', type=int, default=10000000, help='Maximum logfile size in bytes')
    grp.add_argument('--logCount', type=int, default=3, help='Number of backup files to keep')
    grp.add_argument('--verbose', action='store_true', help='Enable verbose logging')

def mkLogger(args:argparse.ArgumentParser) -> logging.Logger:
    logger = logging.getLogger()
    if args.logfile:
        ch = logging.handlers.RotatingFileHandler(args.logfile,
                maxBytes=args.logBytes,
                backupCount=args.logCount)
    else:
        ch = logging.StreamHandler()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s: %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger

class Writer(threading.Thread):
    ''' Wait on a queue, and write the item to a file '''
    def __init__(self, fn:str, logger:logging.Logger):
        threading.Thread.__init__(self, daemon=True)
        self.name = 'Writer'
        self.fn = fn
        self.logger = logger
        self.q = queue.Queue()

    def run(self) -> None:
        '''Called on thread start '''
        fn = self.fn
        if not os.path.isfile(fn): # Create a database
            try:
                self.logger.info('Creating database %s', fn)
                with sqlite3.connect(fn) as conn:
                    conn.execute('CREATE TABLE data(\n' \
                            + '    t REAL PRIMARY KEY,\n' \
                            + '    addr TEXT,\n' \
                            + '    port INTEGER,\n' \
                            + '    info TEXT' \
                            + ');')
                    conn.commit()
            except:
                self.logger.exception('Exception creating %s', fn)
                return

        while True: # Loop forever
            try:
                (t, addr, item) = self.q.get()
                self.q.task_done()
                self.logger.info('t=%s addr=%s:%s Item=%s', t, addr[0], addr[1], item)
                with sqlite3.connect(fn) as conn:
                    self.logger.info('%s', item)
                    conn.execute('INSERT OR REPLACE INTO data VALUES(?,?,?,?);', 
                            (t, addr[0], addr[1], item))
                    conn.commit()
            except:
                self.logger.exception('Exception while writing to %s', fn)

class Parser(threading.Thread):
    ''' Read from a connection, parse it, and send to the output queue '''
    def __init__(self, conn, addr, logger:logging.Logger, q:queue.Queue):
        threading.Thread.__init__(self, daemon=True)
        self.name = 'Parser({}:{})'.format(addr[0], addr[1])
        self.conn = conn
        self.addr = addr
        self.logger = logger
        self.q = q

    def __parser(self, msg:bytes) -> str:
        ''' Parse msg and return a string to be written to a file '''
        self.logger.info('MSG=%s', msg)
        result = msg # Add real parser here
        return result

    def run(self) -> None:
        '''Called on thread start '''
        try:
            msg = b''
            with self.conn as conn:
                t0 = time.time()
                while True: # Get the whole message until the socket is closed
                    data = conn.recv(8192) # Get the data
                    self.logger.debug('data=%s', data)
                    if not data: break # connection has dropped
                    msg += data
            self.q.put((t0, self.addr, self.__parser(msg)))
        except:
            self.logger.exception('Exception while reading from address %s', self.addr)

parser = argparse.ArgumentParser(description="Listen for a GSatMicro message")
addLoggerArgs(parser)
parser.add_argument('--output', type=str, required=True, help='SQLite3 database to use')
grp = parser.add_argument_group('Listener Related Options')
grp.add_argument('--port', type=int, required=True, help='Port to listen on')
grp.add_argument('--maxConnections', type=int, default=10, 
            help='Maximum number of simultaneous connections')
args = parser.parse_args()

logger = mkLogger(args)
logger.info('args=%s', args)

try:
    writer = Writer(args.output, logger) # Create the writer thread
    writer.start() # Start the writer thread

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        logger.debug('Opened socket')
        s.bind(('', args.port))
        logger.debug('Bound to port %s', args.port)
        s.listen()
        logger.debug('Listening to socket')
        while writer.is_alive(): # If writer is alive then try for another connection
            (conn, addr) = s.accept() # Wait for a connection
            logger.info('Connection from %s', addr)
            thrd = Parser(conn, addr, logger, writer.q) # Create a new parser thread
            thrd.start() # Start the new parser thread
            logger.info('n Threads %s', threading.active_count())
            while threading.active_count() > args.maxConnections: # don't overload the system
                time.sleep(1) # Wait for a thread to die before accepting anything else
except:
    logger.exception('Unexpected exception while listening')
