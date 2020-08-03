#! /usr/bin/env python3
#
# Listen/read a glider's call in dialog,
# then generate a goto file and send it to the Dockserver
#
# There are thre methods of listening/reading a dialog file:
#  Using SFMC 8.5's API interface, output_glider_dialog_data.js
#  Reading a file with the API's output
#  Reading a log file
#
# July-2020, Pat Welch, pat@mousebrains.com

import sys
import os
import os.path
import argparse
import logging
import re
import json
import subprocess
import threading
import queue
import copy
import getpass
import socket
from tempfile import NamedTemporaryFile
from smtplib import SMTP
from email.mime.text import MIMEText
from geopy.distance import distance as geodesic
import MyLogger
import WayPoint
from WayPoints import WayPoints
from datetime import datetime, timezone, timedelta
from Dialog import Dialog
from Drifter import Drifter
from Patterns import Patterns

nodeCommand = "/usr/bin/node" # For running javascripts

class Update(threading.Thread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        threading.Thread.__init__(self, daemon=True)
        self.name = "Update"
        self.args = args
        self.logger = logger
        self.__queue = queue.Queue()
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
        grp = parser.add_mutually_exclusive_group(required=True)
        grp.add_argument("--gotoFile", type=str, metavar="filename", help="Write a goto filename")
        grp.add_argument("--gotoAPI", type=str, metavar="dir",
                help="Use SFMC API to update and deploy goto file")
        grp = parser.add_argument_group(description="Make Goto Options")
        grp.add_argument("--gotoArchive", type=str, metavar="dir",
                help="Archive a copy of the generated goto file")
        grp.add_argument("--gotoDT", type=float, default=900, metavar="seconds",
                help="How long will the glider spend on the surface")
        grp.add_argument("--gotoRetain", action="store_true", 
                help="Retain the temporary files sent via API")
        grp.add_argument("--gotoIndex", type=float, metavar="meters",
                help="How close to consider commanded and previous waypoints matching")
        grp.add_argument("--pattern", type=str, required=True, metavar="filename",
                help="YAML file of patterns")
        grp = parser.add_argument_group(description="Mail Options")
        grp.add_argument("--mailTo", type=str, action="append", metavar="foo@bar.com",
                help="EMail address(s) to send a copy of goto to")
        grp.add_argument("--mailFrom", type=str, metavar="foo@bar.com",
                help="Who the email is coming from")
                # default="tpw@ceoas.oregonstate.edu",

    def join(self) -> None:
        self.__queue.join()

    def put(self, msg) -> None:
        self.__queue.put(msg)

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
            logger.info("Patterns\n%s", a)
            logger.info("glider %s", glider)
            logger.info("fn %s", fn)
            if glider not in a:
                raise Exception("Glider, " + glider + ", not in patterns, " + fn)
            self.__pattern = a[glider]['patterns']
            self.__IMEI = a[glider]['IMEI']
            self.__newPattern = True

        return self.__pattern

    def __archive(self, goto:str) -> None:
        if self.args.gotoArchive is None: return
        now = datetime.now(tz=timezone.utc)
        fn = "goto.{:04d}{:02d}{:02d}.{:02d}{:02d}{:02d}.ma".format(
                now.year, now.month, now.day, 
                now.hour, now.minute, now.second)
        fn = os.path.join(self.args.gotoArchive, fn)
        with open(fn, "w") as fp:
            fp.write(goto)

    def __file(self, goto:str) -> None:
        if self.args.gotoFile is None: return
        fn = self.args.gotoFile
        d = os.path.dirname(fn) # Get directory name
        tfn = None
        # Do as an atomic operation
        with NamedTemporaryFile(dir=os.path.dirname(fn), delete=False) as fp:
            fp.write(bytes(goto, 'utf-8'))
            tfn = fp.name
        os.replace(tfn, fn)

    def __apiRun(self, js, *argv) -> bool:
        cmd = [nodeCommand, js]
        cmd.extend(argv)
        a = subprocess.run(cmd, cwd=args.apiDir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if (a.returncode == 0) and (len(a.stdout) == 0):
            return True

        self.logger.error("Error executing %s, rc=%s, %s", " ".join(cmd), a.returncode, a.stdout)
        return False

    def __api(self, goto:str) -> None:
        args = self.args
        if args.gotoAPI is None: return
        fn = None
        with NamedTemporaryFile(dir=args.gotoAPI, 
                prefix="goto_list.", suffix=".ma", delete=False) as fp:
            fp.write(bytes(goto, 'utf-8'))
            fn = fp.name
        if self.__apiRun("update_waypoint_plan.js", args.glider, fn):
            if self.__apiRun("deploy_goto_file.js", args.glider):
                logger.info("Sent goto file for %s", args.glider)

        if not self.args.gotoRetain:
            os.unlink(fn)
        else:
            self.logger.info("Temporary filename %s", fn)

    def __mailTo(self, goto:str) -> None:
        args = self.args
        if args.mailTo is None: return
        try:
            if args.mailFrom is None:
                args.mailFrom = getpass.getuser() + "@" + socket.getfqdn()
                self.logger.info("mailFrom=%s", args.mailFrom)
            msg = []
            msg.append("Ffrom: " + args.mailFrom)
            msg.append("To: " + ",".join(args.mailTo))
            msg.append("Subject: Goto file for " + args.glider)
            msg.append("")
            msg = "\r\n".join(msg)
            msg += goto
            s = SMTP("localhost")
            s.set_debuglevel(1)
            s.sendmail(args.mailFrom, args.mailTo, msg)
            s.quit()
        except:
            self.logger.exception("Error sending mail to %s from %s", 
                    ",".join(args.mailTo), args.mailFrom)

    def __getIndex(self, dialog:Dialog) -> int:
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

    def __mkGoto(self, dialog:Dialog) -> None:
        args = self.args
        logger = self.logger
        pattern = self.__getPattern(args.glider) # Update the patterns if needed
        args.IMEI = self.__IMEI # Which drifter beacon to fetch
        drifter = Drifter(args, logger) # Get drifter information
        glider = dialog.glider() # Where the glider is
        water = dialog.water() # The currents
        now = dialog['t'] # When the glider surfaced
        if now is None: now = datetime.now(tz=timezone.utc) # Unknown, so use now
        dt = self.args.gotoDT # How long the glider will be at the surfac3
        t0 = now + timedelta(seconds=dt) # Next dive time
        d = drifter.estimate(t0) # Estimate where the drifter will be at t0
        dd = WayPoint.Drifter(d.at[0,'lat'], d.at[0,'lon'], d.at[0,'vx'], d.at[0,'vy'])
        dLat = float(d['vy'] * dt / d['latPerDeg']) # How far the glider will drift
        dLon = float(d['vx'] * dt / d['lonPerDeg'])
        glider.latLon.lat += dLat # Estimated dive position
        glider.latLon.lon += dLon
        index = self.__getIndex(dialog)
        self.__newPattern = False
        try:
            self.wpts = WayPoints(dd, glider, water, pattern, index=index) # Make waypoints
            goto = self.wpts.goto()
            logger.info("GOTO\n%s", goto)
            self.__api(goto)
            self.__archive(goto)
            self.__file(goto)
            self.__mailTo(goto)
        except:
            logger.exception("Unable to make waypoints\nDIALOG:\n%s\nDRIFTER:\n%s\n%s", 
                    dialog, d, pattern)


    def run(self) -> None: # Called on start
        args = self.args
        logger = self.logger
        q = self.__queue
        logger.info("Starting")
        while True:
            dialog = q.get()
            try:
                self.__mkGoto(dialog)
                pass
            except:
                logger.exception("Exception while processing dialog\n%s", dialog)
            q.task_done()

class Listener:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger, update:Update) -> None:
        self.args = args
        self.logger = logger
        self.update = update
        self.dialog = Dialog(logger)

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_mutually_exclusive_group(required=True)
        grp.add_argument("--apiListen", action="store_true",
                help="Should the API be used to read the dialog?")
        grp.add_argument("--apiInput", type=str, metavar="filename",
                help="Name of API file to read")
        grp.add_argument("--dialogInput", type=str, metavar="filename",
                help="Name of dialog file to read")
        grp = parser.add_argument_group(description="API options")
        grp.add_argument("--apiDir", type=str, metavar="dir",
                help="Where SFMC's API JavaScripts are located")
        grp.add_argument("--apiCopy", type=str, metavar="filename",
                help="Write out a copy of what is read from the API")
        grp.add_argument("--glider", type=str, required=True, metavar="name",
                help="Name of glider to operate on")

    def __procLine(self, line:str) -> None:
        self.logger.debug("%s", line.strip())
        self.dialog += line
        if self.dialog.flagged():
            self.update.put(copy.copy(self.dialog)) # Spin off to update thread

    def __procBuffer(self, buffer:str, qPartial:bool = False) -> str:
        while len(buffer):
            index = buffer.find("\n")
            if index < 0: # No newline found
                if qPartial:
                    self.__procLine(buffer)
                    return ""
                return buffer
            self.__procLine(buffer[0:(index+1)])
            buffer = buffer[(index+1):]
        return ""

    def __apiLine(self, line:bytearray, buffer:str) -> str:
        a = re.fullmatch(bytes(r"([{].+[}])\x00\n", "utf-8"), line)
        if a is None: return buffer
        msg = json.loads(a[1])
        if "data" not in msg: return buffer
        return self.__procBuffer(buffer + msg["data"], False)

    def __apiListen(self) -> bool:
        args = self.args
        logger = self.logger
        if args.apiDir is None:
            raise Exception("You must specify --apiDir when using --apiListen")

        cmd = (nodeCommand, "output_glider_dialog_data.js", args.glider)
        with subprocess.Popen(cmd,
                cwd=args.apiDir,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT) as p:
            fp = None if args.apiCopy is None else open(args.apiCopy, "wb")
            buffer = ""
            while True:
                line = p.stdout.readline()
                if re.match(b"Error attempting to get glider data", line) is not None:
                    logger.error("ERROR executing\ncmd=%s\ndir=%s\n%s", cmd, args.apiDir, line)
                    raise Exception("Error starting API Listener")

                if (len(line) == 0) and (p.poll() is not None):
                    break # Broken connection
                if fp is not None:
                    fp.write(line)
                    fp.flush()
                buffer = self.__apiLine(line, buffer)
            if fp is not None: fp.close()
            self.__procBuffer(buffer, True)
        return True

    def __apiInput(self, fn:str) -> bool:
        logger = self.logger
        logger.info("Opening %s", fn)
        with open(fn, "rb") as fp:
            buffer= ""
            for line in fp:
                buffer = self.__apiLine(line, buffer)
            self.__procBuffer(buffer, True) # partial line
        return False

    def __dialogInput(self, fn:str) -> bool:
        logger = self.logger
        logger.info("Opening %s", fn)
        with open(fn, "r") as fp:
            buffer = ""
            while True:
                txt = fp.read(1024)
                if len(txt) == 0: # EOF
                    self.__procBuffer(buffer, True)
                    return False
                buffer = self.__procBuffer(buffer + txt)
        return False

    def listen(self) -> bool:
        args = self.args
        if args.apiListen: return self.__apiListen()
        if args.apiInput: return self.__apiInput(args.apiInput)
        return self.__dialogInput(args.dialogInput)

parser = argparse.ArgumentParser(description="Glider Dialog Listener")
MyLogger.addArgs(parser)
Listener.addArgs(parser)
Update.addArgs(parser)
args = parser.parse_args()

logger = MyLogger.mkLogger(args)

logger.info("args=%s", args)

update = Update(args, logger)
listener = Listener(args, logger, update)

update.start() # Start the update thread

try:
    while listener.listen():
        # Listen to the API interface or read in a file
        # Loop if need be for the API interface
        pass 
except:
    logger.exception("Unexpected Exception")

update.join() # Wait for all queued messages to be done
