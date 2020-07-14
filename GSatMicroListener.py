#! /usr/bin/env python3
#
# Listen to a socket
# When a connection is made,
# spawn a thread, parse the message,
# and send to the writer thread.
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import socket
import argparse
import threading
import MyLogger
from Forwarder import Forwarder
from Writer import Writer
from Reader import Reader

parser = argparse.ArgumentParser(description="Listen for a GSatMicro message")
MyLogger.addArgs(parser)
Forwarder.addArgs(parser)
Writer.addArgs(parser)
grp = parser.add_argument_group('Listener Related Options')
grp.add_argument('--port', type=int, required=True, metavar='port', help='Port to listen on')
grp.add_argument('--maxConnections', type=int, default=10, metavar='count',
            help='Maximum number of simultaneous connections')
args = parser.parse_args()

logger = MyLogger.mkLogger(args)
logger.info('args=%s', args)

try:
    fwd = Forwarder(args, logger) # Create a packet forwarder
    fwd.start() # Start the forwarder

    writer = Writer(args, logger) # Create the db writer thread
    writer.start() # Start the writer thread

    queues = [fwd.q, writer.q]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        logger.debug('Opened socket')
        s.bind(('', args.port))
        logger.debug('Bound to port %s', args.port)
        s.listen()
        logger.debug('Listening to socket')
        while writer.is_alive():
            (conn, addr) = s.accept() # Wait for a connection
            logger.info('Connection from %s', addr)
            thrd = Reader(conn, addr, logger, queues) # Create a new reader thread
            thrd.start() # Start the new reader thread
            logger.info('n Threads %s', threading.active_count())
            while threading.active_count() > args.maxConnections: # don't overload the system
                time.sleep(1) # Wait for a thread to die before accepting anything else
except:
    logger.exception('Unexpected exception while listening')
