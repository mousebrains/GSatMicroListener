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
from smtplib import SMTP
from tempfile import NamedTemporaryFile
import WayPoint
from Patterns import Patterns
from Drifter import Drifter
from WayPoints import WayPoints
from MyBaseThread import MyBaseThread

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

    def put(self, glider:str, goto:str) -> None:
        self.__queue.put((glider, goto))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto) = q.get()
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

    def put(self, glider:str, goto:str) -> None:
        self.__queue.put((glider, goto))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto) = q.get()
            logger.debug("glider=%s goto\n%s", glider, goto)
            self.__mailTo(glider, goto)
            q.task_done()

    def __mailTo(self, glider:str, goto:str) -> None:
        args = self.args
        if args.gotoMailTo is None: return
        try:
            if args.gotoMailFrom is None:
                args.gotoMailFrom = getpass.getuser() + "@" + socket.getfqdn()
                self.logger.info("mailFrom=%s", args.gotoMailFrom)
            msg = []
            msg.append("From: " + args.gotoMailFrom)
            msg.append("To: " + ",".join(args.gotoMailTo))
            msg.append("Subject: Goto file for " + glider)
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

    def put(self, glider:str, goto:str) -> None:
        self.__queue.put((glider, goto))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto) = q.get()
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

    def put(self, glider:str, goto:str) -> None:
        self.__queue.put((glider, goto))

    def waitToFinish(self) -> None:
        self.__queue.join() # Don't return until all messages are processed

    def runAndCatch(self) -> None: # Called on start
        logger = self.logger
        q = self.__queue
        logger.info("Starting")

        while True:
            (glider, goto) = q.get()
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
        grp = parser.add_argument_group(description="Make Goto Options")
        grp.add_argument("--gotoDT", type=float, default=900, metavar="seconds",
                help="How long will the glider spend on the surface")
        grp.add_argument("--gotoIndex", type=float, metavar="meters",
                help="How close to consider commanded and previous waypoints matching")
        grp.add_argument("--pattern", type=str, required=True, metavar="filename",
                help="YAML file of patterns")

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
            if glider not in a:
                raise Exception("Glider, " + glider + ", not in patterns, " + fn)
            self.__pattern = a[glider]['patterns']
            self.__IMEI = a[glider]['IMEI']
            self.__newPattern = True

        return self.__pattern


    def __getIndex(self, dialog:dict) -> int:
        if (self.wpts is None) or self.__newPattern:  
            return None # No previous waypoints or it is a new pattern

        args = self.args

        if (args.gotoIndex is None) or (args.gotoIndex <= 0):
            return None # Always go to closest waypoint

        cLatLon = (dialog['latWpt'], dialog['lonWpt'])
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



    def __mkGoto(self, info:dict) -> str:
        args = self.args
        logger = self.logger
        pattern = self.__getPattern(args.glider) # Update the patterns if needed
        args.IMEI = self.__IMEI # Which drifter beacon to fetch
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
            self.wpts = WayPoints(dd, glider, water, pattern, index=index) # Make waypoints
            goto = self.wpts.goto(now)
            return goto
        except:
            logger.exception("Unable to make waypoints\nDIALOG:\n%s\nDRIFTER:\n%s\n%s", 
                    info, d, pattern)
        return None


    def runAndCatch(self) -> None: # Called on start
        args = self.args
        logger = self.logger
        q = self.__queue
        threads = self.__threads

        logger.info("Starting")

        for thr in threads:
            thr.start()

        while True:
            (t, dbName) = q.get()
            logger.debug("t=%s dbName\n%s", t, dbName)
            try:
                info = self.__loadDB(dbName)
                goto = self.__mkGoto(info)
                for thr in threads:
                    thr.put(args.glider, goto)

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
