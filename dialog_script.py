#! /usr/bin/env python3
#
# Monitor the dialog and script events together
#
# Aug-2020, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import threading
import subprocess
from logging import Logger
import time
import re
import json
from queue import Queue
import MyLogger

nodeCommand = "/usr/bin/node"

class Common(threading.Thread):
    def __init__(self, name:str, cmd:str, args:ArgumentParser, logger:Logger, q:Queue) -> None:
        threading.Thread.__init__(self, daemon=True)
        self.name = name
        self.cmd = cmd
        self.glider = args.glider
        self.apiDir = args.dir
        self.logger = logger
        self.q = q
        self.pipe = None

    @staticmethod
    def addArgs(parser:ArgumentParser):
        grp = parser.add_argument_group(description="Common options")
        grp.add_argument("--glider", type=str, default="osusim", help="Name of glider")
        grp.add_argument("--dir", type=str, default="/home/pat/sfmc-rest-programs",
                help="Where SFMC API scripts are")

    def mkPipe(self) -> subprocess.Popen:
        if self.pipe is not None: return self.pipe
        cmd = (nodeCommand, self.cmd, self.glider)
        self.pipe = subprocess.Popen(cmd,
                cwd=self.apiDir,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        self.logger.info("Opened pipe for %s", " ".join(cmd))
        return self.pipe

    def run(self) -> None: # Called on start
        logger = self.logger
        logger.info("Starting")
        expr = re.compile(bytes(r"([{].+[}])\x00\n", "utf-8"))

        try:
            while True:
                pipe = self.mkPipe()
                line = pipe.stdout.readline()
                if (len(line) == 0) and (pipe.poll() is not None):
                    self.pipe = None
                    continue
                logger.info("%s", line)
                a = expr.fullmatch(line)
                if a is not None: 
                    self.process(json.loads(a[1]))
        except:
            logger.exception("Unexpected exception")
        q.put(self.name)

class Events(Common):
    def __init__(self, args:ArgumentParser, logger:Logger, q:Queue) -> None:
        Common.__init__(self, "EVENTS", "output_glider_script_events.js", args, logger, q)

    def process(self, a) -> None:
        if "scriptState" in a:
            self.logger.info("STATE %s", a["scriptState"])

class Dialog(Common):
    def __init__(self, args:ArgumentParser, logger:Logger, q:Queue) -> None:
        Common.__init__(self, "DIALOG", "output_glider_dialog_data.js", args, logger, q)
        self.buffer = ""

    def process(self, a) -> None:
        if "data" not in a: return
        self.buffer += a["data"]
        while len(self.buffer):
            index = self.buffer.find("\n")
            if index < 0: return
            self.logger.info("LINE %s", self.buffer[0:index])
            self.buffer = self.buffer[(index+1):]

parser = ArgumentParser()
Common.addArgs(parser)
MyLogger.addArgs(parser)
args = parser.parse_args()

logger = MyLogger.mkLogger(args)

q = Queue()
events = Events(args, logger, q)
dialog = Dialog(args, logger, q)

events.start()
dialog.start()

msg = q.get()
logger.info("Leaving due to failure in %s", msg)
