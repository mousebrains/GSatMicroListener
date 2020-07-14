#
# Forward a byte array to another host:port
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import threading
import socket
import queue
import argparse
import logging

class Forwarder(threading.Thread):
    ''' Wait on a queue and send the received packets to a host:port '''
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        threading.Thread.__init__(self, daemon=True)
        self.name = "FWD"
        self.hostname = args.hostname
        self.port = args.portForward
        self.logger = logger
        self.q = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group("Packet Forwarding Options")
        grp.add_argument("--hostname", type=str, metavar='host', help="Host to forward packets to")
        grp.add_argument("--portForward", type=int, metavar='port',
                help="Port to forward packets to")

    def run(self) -> None: # Called on thread start
        hostname = self.hostname
        port = self.port
        logger = self.logger
        q = self.q
        logger.info("Starting %s:%s", hostname, port)
        while True:
            (t, addr, msg) = q.get()
            q.task_done()
            if hostname is None or port is None: continue # Do nothing
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