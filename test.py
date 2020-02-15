#! /usr/bin/env python3
#
# Write to a host/port a GSatMicro style message
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import time
import socket
import argparse
import logging

parser = argparse.ArgumentParser(description="Listen for a GSatMicro message")
parser.add_argument('--input', type=str, required=True, help='File to read from')
parser.add_argument('--host', type=str, required=True, help='Host to connect to')
parser.add_argument('--port', type=int, required=True, help='Port to connect to')
parser.add_argument('--preDelay', type=float, help='Delay after connection before sending data')
parser.add_argument('--postDelay', type=float, help='Delay after sending data before closing')

args = parser.parse_args()

logger = logging.getLogger()
logger.info('args=%s', args)

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        logger.info('Opened socket')
        s.connect((args.host, args.port))
        logger.info('Connected to host %s port %s', args.host, args.port)
        if args.preDelay is not None and args.preDelay > 0:
            logger.info('Sleeping %s seconds before sending the data')
            time.sleep(args.preDelay)
        with open(args.input, 'rb') as fp:
            for line in fp:
                s.send(line)
        if args.postDelay is not None and args.postDelay > 0:
            logger.info('Sleeping %s seconds after sending the data')
            time.sleep(args.postDelay)
except:
    logger.exception('Unexpected exception while listening')
