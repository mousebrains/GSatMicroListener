#! /usr/bin/env python3
#
# Go through logfile from GSatMicroListener.py
# and either forward packagets or save into a database
#
import argparse
import re
from datetime import datetime, timezone
import MyLogger
from Forwarder import Forwarder
from Writer import Writer

parser = argparse.ArgumentParser(description="Parse GSatMicroListener logfile")
MyLogger.addArgs(parser)
Forwarder.addArgs(parser)
Writer.addArgs(parser)
grp = parser.add_argument_group('Logfile Parser Related Options')
grp.add_argument('input', nargs='+', type=str, help='Logfiles to parse')
args = parser.parse_args()

logger = MyLogger.mkLogger(args)

try:
    fwd = Forwarder(args, logger) # Create a packet forwarder
    fwd.start() # Start the forwarder

    writer = Writer(args, logger) # Create the db writer thread
    writer.start() # Start the writer thread

    queues = [fwd.q, writer.q]
    expr = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} Writer INFO: "
    expr+= r"t=(.*) "
    expr+= r"addr=(\d+.\d+.\d+.\d+):(\d+) "
    expr+= r"(msg|Item)=(b'.*')"
    expr+= r"\s*"

    for fn in args.input:
        with open(fn, "r") as fp:
            n0 = 0
            n1 = 0
            for line in fp:
                n0 += 1
                a = re.match(expr, line)
                if a is None: continue
                b = re.fullmatch(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6})[-+]00:00", a[1])
                if b is not None:
                    t = datetime.strptime(b[1], "%Y-%m-%d %H:%M:%S.%f")
                elif re.fullmatch(r"\d+.\d*", a[1]) is not None:
                    t = datetime.fromtimestamp(float(a[1]), tz=timezone.utc)
                else:
                    print("Unrecognized time format", a[1])
                    t = a[1]
                payload = eval(a[5])
                addr = a[2]
                port = int(a[3])
                vals = (t, (addr, port), payload)
                fwd.q.put(vals)
                writer.q.put(vals)
                n0 += 2
            logger.info("%s contained %s entries out of %s lines", fn, n1, n0)
except:
    logger.exception('Unexpected exception while parsing logfile')

while fwd.q.join(): pass # Wait for queue to be emptied
while writer.q.join(): pass # Wait for queue to be emptied
