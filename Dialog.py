#! /usr/bin/env python3
#
# Parse a glider dialog and extract information from it
#
# July-2020, Pat Welch, pat@mousebrains.com

import re
import math
import datetime
import logging
import argparse
import queue
import sqlite3
from Update import Update
from MyBaseThread import MyBaseThread

class MyPattern:
    def __init__(self, types:tuple, keys:tuple, expr:str) -> None:
        self.keys = keys if isinstance(keys, tuple) else (keys, )
        self.types = types if isinstance(types, tuple) else ((types,) * len(self.keys))
        if len(self.keys) != len(self.types):
            raise Exception("Length of keys(" + str(self.keys) + 
                    ") != length of types(" + str(self.types) + ")")
        self.expr = re.compile(expr)

    @staticmethod
    def __mkDegrees(x:str) -> float:
        y = float(x)
        yabs = abs(y)
        deg = math.floor(yabs/100) # Degrees
        minutes = yabs % 100 # Minutes
        decDeg = deg + minutes / 60
        return decDeg if y >= 0 else -decDeg

    @staticmethod
    def __strptime(val:str) -> datetime.datetime:
        return datetime.datetime.strptime(val, "%c").replace(tzinfo=datetime.timezone.utc)

    def check(self, line:str, logger:logging.Logger) -> dict:
        a = self.expr.fullmatch(line)
        if a is None: return None

        info = {}
        for index in range(len(self.keys)):
            key = self.keys[index]
            cnv = self.types[index]
            val = a[index + 1]
            try:
                if cnv == "float":
                    info[key] = float(val)
                elif cnv == "int":
                    info[key] = int(val)
                elif cnv == "degMin":
                    info[key] = self.__mkDegrees(val)
                elif cnv == "datetime":
                    info[key] = self.__strptime(val)
                elif cnv == "TRUE":
                    info[key] = True
                else:
                    raise Exception("Unrecognized conversion type, {}".format(cnv))
            except:
                logger.exception("Error converting {} to type {} for {}".format(
                    val, cnv, key))
                return None
        return info

numPattern = r"([+-]?\d*[.]?\d+|[+-]?\d*[.]?\d+[Ee][+-]?\d+)"

patterns = [
        MyPattern("float", "m_avg_speed", r"m_avg_speed[(]m/s[)]\s+" + numPattern),
        MyPattern("datetime", "t",
            r"Curr Time:\s+(\w+\s+\w+\s+\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+MT:\s*\d+"),
        MyPattern(("degMin", "degMin", "float"), ("lat", "lon", "dtLatLon"),
            r"GPS\s+Location:\s+" + numPattern + r"\s+[NS]\s+" +
            numPattern + r"\s+[EW]\s+measured\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("degMin", "float"), ("c_wpt_lat", "c_wpt_lat_dt"),
            r"sensor:c_wpt_lat[(]lat[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("degMin", "float"), ("c_wpt_lon", "c_wpt_lon_dt"),
            r"sensor:c_wpt_lon[(]lon[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("float", "float"), ("m_water_vx", "m_water_vx_dt"),
            r"sensor:m_water_vx[(]m/s[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("float", "float"), ("m_water_vy", "m_water_vy_dt"),
            r"sensor:m_water_vy[(]m/s[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern("TRUE", "FLAG", r"s \*[.](sbd|tbd) \*[.](sbd|tbd)"),
        ] 

class Dialog(MyBaseThread):
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger):
        MyBaseThread.__init__(self, "Dialog", args, logger)
        self.__queue = queue.Queue()
        self.__update = Update(args, logger)

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        Update.addArgs(parser)
        grp = parser.add_argument_group(description="Dialog related options")
        grp.add_argument("--gliderDB", type=str, metavar="filename", required=True,
                help="Name of glider database")

    def __repr__(self) -> str:
        msg = []
        for key in sorted(self):
            msg.append("{}={}".format(key, self[key]))
        return "\n".join(msg)

    def put(self, line:str) -> None:
        self.__queue.put((datetime.datetime.now(tz=datetime.timezone.utc), line))

    def runAndCatch(self) -> None: # Called on start
        args = self.args
        logger = self.logger
        q = self.__queue
        update = self.__update

        logger.info("Starting")

        update.start()

        db = None
        timeout = 300 # Close database if no communications for this long
        sql = "INSERT OR REPLACE INTO glider VALUES(?,?,?);"

        while True:
            try:
                (t, line) = q.get(timeout=None if db is None else timeout)
                line = line.strip()
                logger.debug("t=%s line=%s", t, line)
                for pattern in patterns:
                    info = pattern.check(line, logger)
                    if info is None: continue
                    if db is None:
                        db = self.__makeDB(args.gliderDB)
                        cur = db.cursor()
                    for key in info:
                        cur.execute(sql, (t, key, info[key]))
                    db.commit()
                    if "FLAG" in info: update.put(t, args.gliderDB)
                    break
                q.task_done()
            except queue.Empty:
                if db is not None:
                    logger.info("Closing database")
                    db.close()
                    db = None
            except:
                logger.exception("Error processing %s", line)
                q.task_done()

    def __makeDB(self, dbName:str) -> None:
        sql = "CREATE TABLE IF NOT EXISTS glider ( -- Glider dialog information\n"
        sql+= "    t TIMESTAMP WITH TIME ZONE, -- When line was received\n"
        sql+= "    name TEXT, -- name of the field\n"
        sql+= "    val FLOAT, -- value of the field\n"
        sql+= "    PRIMARY KEY (t,name) -- Retain each t/name pair\n"
        sql+= ");"
        db = sqlite3.connect(dbName)
        cur = db.cursor()
        cur.execute(sql)
        return db

    def waitToFinish(self) -> None:
        self.__queue.join()
        self.__update.waitToFinish()

if __name__ == "__main__":
    import argparse
    import MyLogger
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("fn", nargs="+", metavar="dialog(s)", help="SFMC dialog file(s)")
    parser.add_argument("--glider", type=str, required=True, help="Name of glider")
    Dialog.addArgs(parser)
    MyLogger.addArgs(parser)
    args = parser.parse_args()

    logger = MyLogger.mkLogger(args)
    dialog = Dialog(args, logger)
    dialog.start()

    for fn in args.fn:
        logger.info("Working on %s", fn)
        with open(fn, "r") as fp:
            for line in fp:
                dialog.put(line)
                time.sleep(0.001)

    dialog.waitToFinish()
