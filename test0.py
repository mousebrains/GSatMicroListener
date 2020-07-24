#! /usr/bin/env python3
#
# Test out loading a YAML file and generating waypoints
#
# July-2020, Pat Welch, pat@mousebrains.com

from Patterns import Patterns
from WayPoints import WayPoints
import WayPoint
import argparse
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("fn", nargs="+", metavar="fn.yaml", help="YAML file(s)")
args = parser.parse_args()

drifter = WayPoint.Drifter(44, -124, 0.0, 0.1)
glider = WayPoint.Glider(44.01, -124, 0.4)
water = WayPoint.Water(-0.1, 0.1)
t0 = datetime.datetime(2020,7,1)

for fn in args.fn:
    patterns = Patterns(fn)
    for name in sorted(patterns):
        item = patterns[name]
        print(name, item)
        wpts = WayPoints(drifter, glider, water, item['patterns'], index=None)
        print("index", wpts.index)
        print(wpts.goto(t0))
