#! /usr/bin/env python3
#
# Send fake drift GPS fix packets
#
# July-2020, Pat Welch, pat@mousebrains.com

import argparse
import logging
import MyLogger
import time
import math
import random
from datetime import datetime, timezone
from Forwarder import Forwarder
from geopy.distance import distance
from BitArray import BitArray


class Header:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        self.cdr = 0
        self.imei = b'300234068117290'
        self.momsn = 0
        self.mtmsn = 0

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        pass

    def encode(self) -> bytes:
        self.cdr += 2
        self.momsn += 1
        a = bytearray()
        a += (1).to_bytes(1, "big") # 1 -> MO Direct IP Header
        a += (0).to_bytes(2, "big") # Length filled in below
        a += (self.cdr).to_bytes(4, "big") # CDR reference
        a += self.imei              # IMEI should be 15 bytes
        a += (1).to_bytes(1, "big") # Session status
        a += (self.momsn).to_bytes(2, "big") # MOM sequence number
        a += (self.mtmsn).to_bytes(2, "big") # MTM sequence number
        a += (int(time.time())).to_bytes(4, "big") # Time of MOM
        a[1:3] = (len(a) - 3).to_bytes(2, "big") # Fill in the length
        return a

class Location:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        self.lat =   44. # decimal degrees
        self.lon = -124. # decimal degrees
        self.radius = 4  # km

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        pass

    @staticmethod
    def __degMin(x) -> bytes:
        x = abs(x)
        deg = math.floor(x)
        minutes = int((x % 1) * 60 * 1000)
        a = bytearray()
        a += (deg).to_bytes(1, "big")
        a += (minutes).to_bytes(2, "big")
        return a

    def encode(self) -> bytes:
        a = bytearray()
        a += (3).to_bytes(1, "big") # 3 -> MO DirectIP Location
        a += (0).to_bytes(2, "big") # Length filled in below
        flg = int(0)
        if self.lat < 0: flg += 2
        if self.lon < 0: flg += 1
        a += (flg).to_bytes(1, "big") # First byte
        a += self.__degMin(self.lat)
        a += self.__degMin(self.lon)
        a += (self.radius).to_bytes(4, "big")
        a[1:3] = (len(a) - 3).to_bytes(2, "big") # Fill in the length
        return a

class Payload:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        self.logger = logger
        self.lat = args.lat
        self.lon = args.lon
        self.speed = args.spd
        self.spdSigma = args.spdSigma
        self.heading = args.hdg
        self.hdgSigma = args.hdgSigma
        self.altitude = args.altitude
        self.battery = args.battery
        self.batteryRate = args.batteryRate / 3600 # %/hour -> %/sec
        self.t = None
        self.latPerDeg = distance((self.lat-0.5, self.lon), (self.lat+0.5, self.lon)).meters
        self.lonPerDeg = distance((self.lat, self.lon-0.5), (self.lat, self.lon+0.5)).meters
        if args.seed is not None:
            random.seed(args.seed)

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Drifter options")
        grp.add_argument("--lat", type=float, default=44.75, metavar="lat",
                help="Initial latitude in decimal degrees")
        grp.add_argument("--lon", type=float, default=-125, metavar="lon",
                help="Initial longitude in decimal degrees")
        grp.add_argument("--spd", type=float, default=0.1, metavar="m/s",
                help="Central value of speed in m/sec")
        grp.add_argument("--spdSigma", type=int, default=0.02, metavar="m/s",
                help="Gaussian norm noise width of speed in m/sec")
        grp.add_argument("--hdg", type=float, default=60, metavar="degrees",
                help="Central drifter heading in degrees from true north")
        grp.add_argument("--hdgSigma", type=float, default=10, metavar="degrees",
                help="Gaussian norm noise width of heading in m/sec")
        grp.add_argument("--altitude", type=int, default=0, metavar="m", help="Altitude in m")
        grp.add_argument("--battery", type=float, default=96, metavar="%", help="Battery %%")
        grp.add_argument("--batteryRate", type=float, default=22, metavar="%/day", 
                help="Battery %%/day")
        grp.add_argument("--seed", type=int, metavar='int', help='Random seed')

    def __repr__(self) -> str:
        msg = "t={}".format(datetime.fromtimestamp(self.t, tz=timezone.utc))
        msg+= " lat={} lon={}".format(self.lat, self.lon)
        msg+= " spd={} hdg={}".format(self.speed, self.heading)
        msg+= " bat={}".format(self.battery)
        return msg

    def __move(self) -> None:
        """ Move the Drifter """
        t = time.time()
        if self.t is None: # Initial, so do nothing
            self.t = t
        dt = t - self.t
        self.t = t
        self.speed = abs(random.gauss(self.speed, self.spdSigma))
        self.heading = random.gauss(self.heading, self.hdgSigma) % 360
        self.battery = max(0, self.battery - self.batteryRate * dt)
        dist = self.speed * dt
        theta = math.radians(self.heading)
        dNorth = dist * math.cos(theta)
        dEast  = dist * math.sin(theta)
        dLat = dNorth / self.latPerDeg
        dLon = dEast / self.lonPerDeg
        self.lat += dLat
        self.lon += dLon

    def __gps18(self) -> bytes:
        lat = round((self.lat +  90) * 186413)
        lon = round((self.lon + 180) * 186413)
        tRef = datetime(2015,1,1,0,0,0, tzinfo=timezone.utc)
        t = datetime.fromtimestamp(self.t, tz=timezone.utc)
        dt = (t - tRef).total_seconds()
        bits = BitArray(b"")
        bits.append(3, 0) # Magic
        bits.append(26, lon) # Longitude
        bits.append(1, 0) # External Power
        bits.append(1, 0) # Distress
        bits.append(1, 0) # Checkin
        bits.append(29, round(dt)) # seconds since 2015-1-1
        bits.append(3, 6) # Number of sattelites
        bits.append(25, lat) # Latitude
        bits.append(6, round(self.heading / 5)) # Heading in deg/5
        bits.append(6, 4) # Accuracy in meters
        bits.append(11, 0) # Climb rate in m/sec
        bits.append(5, min(31, round(self.battery/3))) # Battery percentage/3
        bits.append(11, round(self.speed / 1000 * 3600)) # Speed in kph
        bits.append(16, self.altitude) # Altitude in meters

        return bits.to_bytes()
        
    def encode(self) -> bytes:
        self.__move() # Move the drifter
        body = self.__gps18()
        a = bytearray()
        a += (2).to_bytes(1, "big") # 3 -> MO DirectIP Location
        a += (0).to_bytes(2, "big") # Length filled in below
        a += (5).to_bytes(1, "big") # 18 byte GPS message format
        a += self.__gps18() # Build 18 byte payload
        a[1:3] = (len(a) - 3).to_bytes(2, "big") # Fill in the length
        self.logger.info("GPS %s", self)
        return a

class FauxDrifter:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        self.args = args
        self.logger = logger
        self.hdr = Header(args, logger)
        self.location = Location(args, logger)
        self.payload = Payload(args, logger)

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        Header.addArgs(parser)
        Location.addArgs(parser)
        Payload.addArgs(parser)
        pass

    def mkMessage(self) -> bytearray:
        args = self.args
        logger = self.logger
        msg = bytearray()
        msg += (1).to_bytes(1, "big") # 1 -> MOM Version
        msg += (0).to_bytes(2, "big") # Length filled in below
        msg += self.hdr.encode() #  MO DirectIP Header
        msg += self.location.encode() # MO DirectIP Location
        msg += self.payload.encode() # MO DirectIP GPS Payload
        msg[1:3] = (len(msg) - 3).to_bytes(2, "big") # Fill in the length
        self.logger.info("n=%s msg=%s", len(msg), bytes(msg))
        return bytes(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Drifter calculation from GPS fixes")
    parser.add_argument("--dt", type=int, default=15*60, metavar="seconds",
            help="Time between packets")
    FauxDrifter.addArgs(parser)
    Forwarder.addArgs(parser)
    MyLogger.addArgs(parser)
    args = parser.parse_args()
    logger = MyLogger.mkLogger(args)
    faux = FauxDrifter(args, logger)

    forwarder = Forwarder(args, logger)
    forwarder.start()

    while True:
        msg = faux.mkMessage()
        forwarder.put(msg)
        time.sleep(args.dt)
