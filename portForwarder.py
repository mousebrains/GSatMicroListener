#! /usr/bin/env python3
#
# Forward connections to one port to another, possibly on a different machine.
#
#
# Feb-2020, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
import logging.handlers
import getpass
import socket
import time
import queue
import sys
import threading

def addArgsLogger(parser:ArgumentParser) -> None:
    grp = parser.add_argument_group('Logger Related Options')
    grp.add_argument('--logfile', type=str, metavar='filename', help='Name of logfile')
    grp.add_argument('--logBytes', type=int, default=10000000, metavar='length',
            help='Maximum logfile size in bytes')
    grp.add_argument('--logCount', type=int, default=3, metavar='count',
            help='Number of backup files to keep')
    grp.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    grp.add_argument("--mailTo", action="append", metavar="foo@bar.com",
            help="Where to mail errors and exceptions to")
    grp.add_argument("--mailFrom", type=str, metavar="foo@bar.com",
            help="Who the mail originates from")
    grp.add_argument("--mailSubject", type=str, metavar="subject",
            help="Mail subject line")
    grp.add_argument("--smtpHost", type=str, default="localhost", metavar="foo.bar.com",
            help="SMTP server to mail to")

def mkLogger(args:ArgumentParser) -> logging.Logger:
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

    if args.mailTo is not None:
        frm = args.mailFrom if args.mailFrom is not None else \
                (getpass.getuser() + "@" + socket.getfqdn())
        subj = args.mailSubject if args.mailSubject is not None else \
                ("Error on " + socket.getfqdn())

        ch = logging.handlers.SMTPHandler(args.smtpHost, frm, args.mailTo, subj)
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

class Forwarder(threading.Thread):
    def __init__(self, conn, addr:tuple, args:ArgumentParser, logger:logging.Logger) -> None:
        threading.Thread.__init__(self, daemon=True)
        self.name = "FWD:{}".format(addr)
        self.conn = conn
        self.addr = addr
        self.hostname = args.hostname
        self.port = args.portForward
        self.logger = logger
    
    @staticmethod
    def addArgs(parser:ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Forwarder options")
        grp.add_argument("--hostname", type=str, required=True, metavar="foo.bar.com",
                help="Hostname to forward packets to")
        grp.add_argument("--portForward", type=int, required=True, metavar="port",
                help="Port on hostname to forward to")

    def run(self) -> None: # Called on thread start
        conn = self.conn
        logger = self.logger
        tgt = "{}:{}".format(self.hostname, self.port)
        logger.info("Starting %s -> %s", self.addr, tgt)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                logger.debug("Opened socket")
                s.connect((self.hostname, self.port))
                logger.debug("Connected to %s", tgt)
                while True:
                    msg = conn.recv(1024*1024)
                    logger.debug("Recevied data length %s from %s", len(msg), self.addr)
                    if not msg: break # Connection has dropped 
                    while len(msg): # forward the packet
                        n = s.send(msg)
                        if n == 0:
                            logger.error("Socket connection to %s broken", tgt)
                            return
                        msg = msg[n:]
        except:
            logger.exception("Exception %s -> %s", self.addr, tgt)


parser = ArgumentParser(description="Forward packets from one port to another")
addArgsLogger(parser)
Forwarder.addArgs(parser)
grp = parser.add_argument_group(description="Listener related options")
grp.add_argument("--port", type=int, required=True, metavar="port", help="Port to listen on")
grp.add_argument("--maxConnections", type=int, default=10, metavar="count",
        help="Maximum number of simultaneous connections")
args = parser.parse_args()

logger = mkLogger(args)
logger.info("args=%s", args)

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        logger.debug("Opened socket")
        s.bind(("", args.port))
        logger.debug("Bound to port %s", args.port)
        s.listen()
        logger.debug("Listening to socket for incoming connections")
        while True:
            (conn, addr) = s.accept() # Wait for a connection request
            logger.info("Connection from %s", addr)
            thrd = Forwarder(conn, addr, args, logger)
            thr.start()
            logger.info("n Threads %s", threading.active_count())
            while threading.active_count() > (args.maxConnections + 1):
                time.sleep(1) # Wait for a thread to die before accepting anything else
except:
    logger.exception("Unexpected exception while listening for a connection")
    sys.exit(1)
