#
# Construct a logger object
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import argparse
import logging
import logging.handlers
import getpass
import socket

def addArgs(parser:argparse.ArgumentParser) -> None:
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
