#
# Forward a byte array to another host:port
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import socket
import queue
import argparse
import logging
from MyBaseThread import MyBaseThread

class Forwarder(MyBaseThread):
    ''' Wait on a queue and send the received packets to a host:port '''
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThreadThread.__init__(self, "FWD", args, logger)
        self.hostname = args.hostname
        self.port = args.portForward
        self.q = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group("Packet Forwarding Options")
        grp.add_argument("--hostname", type=str, metavar='host', help="Host to forward packets to")
        grp.add_argument("--portForward", type=int, metavar='port',
                help="Port to forward packets to")

    def put(self, msg) -> None:
        t = None
        addr = None
        self.q.put((t, addr, msg))

    def runAndCatch(self) -> None: # Called on thread start
        hostname = self.hostname
        port = self.port
        logger = self.logger
        q = self.q
        logger.info("Starting %s:%s", hostname, port)
        while True:
            (t, addr, msg) = q.get()
            if hostname is None or port is None: # Do nothing
                q.task_done() # I'm done processing this message
                continue # Do nothing
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    logger.debug("Opened socket")
                    s.connect((hostname, port))
                    logger.debug("Connected to {}:{}", hostname, port)
                    while len(msg):
                        n = s.send(msg)
                        if n == 0:
                            logger.error("Socket connection to %s:%s broken", hostname, port)
                            break
                        msg = msg[n:]
            except:
                logger.exception("Error sending to %s:%s", hostname, port)
            q.task_done() # I'm done processing this message
