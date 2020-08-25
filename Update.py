#! /usr/bin/env python3
#
# Read in glider state information from a database and
# generate a goto file to follow a particular pattern
#
# July-2020, Pat Welch, pat@mousebrains.com

import os
import os.path
import argparse
import logging
import queue
import sqlite3
import datetime
import getpass
import socket
import subprocess
import math
from smtplib import SMTP
from tempfile import NamedTemporaryFile
import WayPoint
from Patterns import Patterns
from Drifter import Drifter
from WayPoints import WayPoints
from MyBaseThread import MyBaseThread
from geopy.distance import distance as geodesic

class API(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "API", args, logger)
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Make Goto API Options")
        grp.add_argument("--gotoAPI", type=str, metavar="dir",
                help="Use SFMC API to update and deploy goto file")
        grp.add_argument("--gotoRetain", action="store_true", 
                help="Retain the temporary files sent via API")
        grp.add_argument("--nodeCommand", type=str, metavar="filename", default="/usr/bin/node",
                help="Full path to node command")

    def put(self, glider:str, goto:str, maxDist:float) -> None:
        self.__queue.put((glider, goto, maxDist))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto, maxDist) = q.get()
            if goto is not None:
                logger.debug("glider=%s goto\n%s", glider, goto)
                self.__api(glider, goto)
            q.task_done()

    def __apiRun(self, js, *argv) -> bool:
        args = self.args
        cmd = [args.nodeCommand, js]
        cmd.extend(argv)
        a = subprocess.run(cmd, cwd=args.apiDir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if (a.returncode == 0) and (len(a.stdout) == 0):
            return True

        self.logger.error("Error executing %s, rc=%s, %s", " ".join(cmd), a.returncode, a.stdout)
        return False

    def __api(self, glider, goto:str) -> None:
        args = self.args
        logger = self.logger
        if args.gotoAPI is None: return
        fn = None
        with NamedTemporaryFile(dir=args.gotoAPI, 
                prefix="goto_list.", suffix=".ma", delete=False) as fp:
            fp.write(bytes(goto, 'utf-8'))
            fn = fp.name
        if self.__apiRun("update_waypoint_plan.js", glider, fn):
            if self.__apiRun("deploy_goto_file.js", glider):
                logger.info("Sent goto file for %s", glider)

        if not self.args.gotoRetain:
            os.unlink(fn)
        else:
            logger.info("Temporary filename %s", fn)

class MailTo(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "MailTo", args, logger)
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Make Goto MailTo Options")
        grp.add_argument("--gotoMailTo", type=str, action="append", metavar="foo@bar.com",
                help="EMail address(s) to send a copy of goto to")
        grp.add_argument("--gotoMailFrom", type=str, metavar="foo@bar.com",
                help="Who the email is coming from")

    def put(self, glider:str, goto:str, maxDist:float) -> None:
        self.__queue.put((glider, goto, maxDist))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto, maxDist) = q.get()
            if goto is None:
                if maxDist is None:
                    goto = "Both goto and maxDist are None, probably no valid solution"
                    subject = "{} has both goto and maxDist are Nones"
                else:
                    goto = "{} does not need a new goto file\n".format(glider)
                    goto+= "Maximum distance from old to new waypoints was {}m".format(maxDist)
                    subject = "{} does not need a new goto".format(glider)
            else:
                subject = "Goto file for {}".format(glider)

            self.__mailTo(goto, subject)
            q.task_done()

    def __mailTo(self, goto:str, subject:str) -> None:
        args = self.args
        if args.gotoMailTo is None: return
        try:
            if args.gotoMailFrom is None:
                args.gotoMailFrom = getpass.getuser() + "@" + socket.getfqdn()
                self.logger.debug("mailFrom=%s", args.gotoMailFrom)
            msg = []
            msg.append("From: " + args.gotoMailFrom)
            msg.append("To: " + ",".join(args.gotoMailTo))
            msg.append("Subject: " + subject)
            msg.append("")
            msg = "\r\n".join(msg)
            msg += goto
            s = SMTP("localhost")
            s.set_debuglevel(1)
            s.sendmail(args.gotoMailFrom, args.gotoMailTo, msg)
            s.quit()
        except:
            self.logger.exception("Error sending mail to %s from %s", 
                    ",".join(args.gotoMailTo), args.gotoMailFrom)

class Archiver(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "Archiver", args, logger)
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Make Goto Archiver Options")
        grp.add_argument("--gotoArchive", type=str, metavar="dir",
                help="Archive a copy of the generated goto file")

    def put(self, glider:str, goto:str, maxDist:float) -> None:
        self.__queue.put((glider, goto, maxDist))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto, maxDist) = q.get()
            if goto is not None:
                logger.debug("glider=%s goto\n%s", glider, goto)
                self.__archiver(glider, goto)
            q.task_done()

    def __archiver(self, glider:str, goto:str) -> None:
        if self.args.gotoArchive is None: return
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        fn = "goto.{}.{:04d}{:02d}{:02d}.{:02d}{:02d}{:02d}.ma".format(
                glider,
                now.year, now.month, now.day, 
                now.hour, now.minute, now.second)
        fn = os.path.join(self.args.gotoArchive, fn)
        with open(fn, "w") as fp:
            fp.write(goto)
        self.logger.info("Archived to %s", fn)

class Filer(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "Filer", args, logger)
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Make Goto File Options")
        grp.add_argument("--gotoFile", type=str, metavar="filename", help="Write a goto filename")

    def put(self, glider:str, goto:str, maxDist:float) -> None:
        self.__queue.put((glider, goto, maxDist))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto, maxDist) = q.get()
            if goto is not None:
                logger.debug("glider=%s goto\n%s", glider, goto)
                self.__filer(glider, goto)
            q.task_done()

    def __filer(self, glider:str, goto:str) -> None:
        fn = self.args.gotoFile
        if fn is None: return
        with open(fn, "w") as fp:
            fp.write(goto)

class Update(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "Update", args, logger)
        self.__queue = queue.Queue()
        self.__threads = [
                API(args, logger),
                MailTo(args, logger),
                Archiver(args, logger),
                Filer(args, logger),
                ]

        self.__pattern = None
        self.__patternTime = None
        self.__IMEI = None
        self.__newPattern = True
        self.wpts = None
        if args.gotoAPI is not None:
            if args.apiDir is None:
                raise Exception("--apiDir must be specified with --gotoAPI")
            if not os.path.isdir(args.apiDir):
                raise Exception("--apiDir must be a directory, " + args.apiDir)

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        Drifter.addArgs(parser)
        API.addArgs(parser)
        MailTo.addArgs(parser)
        Archiver.addArgs(parser)
        Filer.addArgs(parser)
        WayPoints.addArgs(parser)
        grp = parser.add_argument_group(description="Make Goto Options")
        grp.add_argument("--gotoDT", type=float, default=900, metavar="seconds",
                help="How long will the glider spend on the surface")
        grp.add_argument("--gotoIndex", type=float, metavar="meters",
                help="How close to consider commanded and previous waypoints matching")
        grp.add_argument("--pattern", type=str, required=True, metavar="filename",
                help="YAML file of patterns")
        grp.add_argument("--gotoTau", type = float, default=4*3600, metavar="seconds",
                help="1/e weighting for speed weight in time")

    def waitToFinish(self) -> None:
        self.__queue.join()
        for thr in self.__threads:
            thr.waitToFinish()

    def put(self, t, dbName) -> None:
        self.__queue.put((t, dbName))

    def __getPattern(self, glider:str) -> Patterns:
        args = self.args
        logger = self.logger
        fn = args.pattern
        if not os.path.isfile(fn):
            raise Exception("Pattern file, " + fn + ", does not exist")
        t = os.path.getmtime(fn)
        if (self.__pattern is None) or (t > self.__patternTime):
            a = Patterns(fn)
            self.__patternTime = t
            if not a.qGlider(glider):
                raise Exception("Glider, " + glider + ", not in patterns, " + fn)
            self.__patternEnabled = a.qEnabled(glider)
            self.__pattern = a.pattern(glider)
            self.__IMEI = a.IMEI(glider)
            self.__newPattern = True

        return self.__pattern


    def __getIndex(self, info:dict) -> int:
        if (self.wpts is None) or self.__newPattern:  
            return None # No previous waypoints or it is a new pattern

        args = self.args

        if (args.gotoIndex is None) or (args.gotoIndex <= 0):
            return None # Always go to closest waypoint

        if ('c_wpt_lat' not in info) or ('c_wpt_lon' not in info):
            return None # current waypoint unknown

        cLatLon = (info['c_wpt_lat'], info['c_wpt_lon'])
        minDistance = args.gotoIndex
        minIndex = None
        for (wpt, dt, index) in self.wpts:
            wpt = wpt.wpt # Actual lat/lon pair
            wptLatLon = (wpt.lat, wpt.lon)
            dist = geodesic(cLatLon, wptLatLon).meters
            if dist < minDistance:
                minDistance = dist
                minIndex = index
        return minIndex

    def __loadDB(self, dbName) -> dict:
        sql = "SELECT name,val FROM glider "
        sql+= "INNER JOIN "
        sql+= "(SELECT name AS nameMax, max(t) AS tMax FROM glider GROUP BY name) "
        sql+= "ON name=nameMax AND t=tMax;"
        db = sqlite3.connect(dbName)
        cur = db.cursor()
        cur.execute(sql)
        info = {}
        for row in cur:
            (key, val) = row
            if isinstance(val, str):
                try:
                    fmt = "%Y-%m-%d %H:%M:%S+00:00"
                    tz = datetime.timezone.utc
                    val = datetime.datetime.strptime(val, fmt).replace(tzinfo=tz)
                except:
                    pass
            info[key] = val

        # Special handling to m_avg_speed where we do a longer term average
        sql = "SELECT strftime('%s',t) as t,val FROM glider"
        sql+= " WHERE name='m_avg_speed'"
        sql+= " ORDER BY t DESC"
        sql+= " LIMIT 20;"
        cur.execute(sql)
        times = []
        speeds = []
        for (t, spd) in cur:
            times.append(float(t))
            speeds.append(float(spd))

        tMax = max(times)
        tau = self.args.gotoTau
        denom = 0
        numer = 0
        for i in range(len(times)):
            t = times[i]
            spd = speeds[i]
            wght = math.exp((t - tMax) / tau)
            denom += wght
            numer += spd * wght

        if denom != 0:
            spd = numer / denom
            self.logger.info("m_avg_speed %s -> %s n=%s", info['m_avg_speed'], spd, len(times))
            info['m_avg_speed'] = spd

        return info

    @staticmethod
    def __mkGlider(info:dict) -> WayPoint.Glider:
        lat = info["lat"] if "lat" in info else None
        lon = info["lon"] if "lon" in info else None
        spd = info["m_avg_speed"] if "m_avg_speed" in info else 0.3
        return WayPoint.Glider(lat, lon, spd)

    @staticmethod
    def __mkWater(info:dict) -> WayPoint.Glider:
        vx = info["m_water_vx"] if "m_water_vx" in info else None
        vy = info["m_water_vy"] if "m_water_vy" in info else None
        return WayPoint.Water(vx, vy)



    def __mkGoto(self, info:dict) -> tuple:
        args = self.args
        logger = self.logger
        pattern = self.__pattern # Patterns to use
        drifter = Drifter(args, logger) # Get drifter information
        glider = self.__mkGlider(info) # Where the glider is
        water = self.__mkWater(info) # The currents
        now = info["t"] if "t" in info else None # When the glider surfaced
        if now is None: now = datetime.datetime.now(tz=datetime.timezone.utc) # Unknown, so use now
        dt = self.args.gotoDT # How long the glider will be at the surfac3
        t0 = now + datetime.timedelta(seconds=dt) # Next dive time
        d = drifter.estimate(t0) # Estimate where the drifter will be at t0
        dd = WayPoint.Drifter(d.at[0,'lat'], d.at[0,'lon'], d.at[0,'vx'], d.at[0,'vy'])
        dLat = float(d['vy'] * dt / d['latPerDeg']) # How far the glider will drift
        dLon = float(d['vx'] * dt / d['lonPerDeg'])
        glider.latLon.lat += dLat # Estimated dive position
        glider.latLon.lon += dLon
        index = self.__getIndex(info)
        self.__newPattern = False
        try:
            self.wpts = WayPoints(dd, glider, water, pattern, args, logger, 
                    index=index) # Make waypoints
            (goto, maxDist) = self.wpts.goto(now, self.__IMEI)
            return (goto, maxDist)
        except:
            logger.exception("Unable to make waypoints\nDIALOG:\n%s\nDRIFTER:\n%s\n%s", 
                    info, d, pattern)
        return (None, None)


    def runAndCatch(self) -> None: # Called on start
        args = self.args
        logger = self.logger
        q = self.__queue
        threads = self.__threads
        for thr in threads: # Start my threads
            thr.start()

        logger.info("Starting")

        while True:
            (t, dbName) = q.get()
            logger.debug("t=%s dbName\n%s", t, dbName)
            try:
                pattern = self.__getPattern(args.glider) # Update the patterns if needed
                args.IMEI = self.__IMEI # Which drifter beacon to fetch
            except:
                logger.exception("Error getting patterns for %s", args.glider)
                q.task_done()
                continue
            if self.__patternEnabled:
                try:
                    info = self.__loadDB(dbName)
                    (goto, maxDist) = self.__mkGoto(info)
                    for thr in threads:
                        thr.put(args.glider, goto, maxDist)
                except:
                    logger.exception("Exception while updating")
            q.task_done()

if __name__ == "__main__":
    import MyLogger
    parser = argparse.ArgumentParser(description="Glider Updater")
    parser.add_argument("--glider", type=str, required=True, help="Name of glider")
    Update.addArgs(parser)
    MyLogger.addArgs(parser)
    args = parser.parse_args()

    logger = MyLogger.mkLogger(args)

    logger.info("args=%s", args)

    update = Update(args, logger)
    update.start() # Start the update thread

    try:
        update.waitToFinish()
    except:
        logger.exception("Unexpected Exception")
