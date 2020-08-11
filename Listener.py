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

import argparse
import logging
import re
import json
import subprocess
import MyLogger
from Dialog import Dialog

class Listener:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger, dialog:Dialog) -> None:
        self.args = args
        self.logger = logger
        self.dialog = dialog

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_mutually_exclusive_group(required=True)
        grp.add_argument("--apiListen", action="store_true",
                help="Should the API be used to read the dialog?")
        grp.add_argument("--apiInput", type=str, metavar="filename",
                help="Name of API file to read")
        grp.add_argument("--dialogInput", type=str, action='append', metavar="filename",
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
        self.dialog.put(line)

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

        cmd = (self.args.nodeCommand, "output_glider_dialog_data.js", args.glider)
        with subprocess.Popen(cmd,
                cwd=args.apiDir,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT) as p:
            fp = None if args.apiCopy is None else open(args.apiCopy, "ab")
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
        for fn in args.dialogInput:
            self.__dialogInput(fn)
        return False

parser = argparse.ArgumentParser(description="Glider Dialog Listener")
MyLogger.addArgs(parser)
Listener.addArgs(parser)
Dialog.addArgs(parser)
args = parser.parse_args()

logger = MyLogger.mkLogger(args)

logger.info("args=%s", args)

dialog = Dialog(args, logger)
dialog.start() # Start the update thread

listener = Listener(args, logger, dialog)

try:
    while listener.listen():
        # Listen to the API interface or read in a file
        # Loop if need be for the API interface
        pass 
except:
    logger.exception("Unexpected Exception")

dialog.join() # Wait for all queued messages to be done
