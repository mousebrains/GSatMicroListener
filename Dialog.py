#! /usr/bin/env python3
#
# Parse a glider dialog and extract information from it
#
# July-2020, Pat Welch, pat@mousebrains.com

import re
import math
import datetime
import logging
import WayPoint

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

    def check(self, line:str, obj:dict, logger:logging.Logger) -> bool:
        a = self.expr.fullmatch(line)
        if a is None: return False

        for index in range(len(self.keys)):
            key = self.keys[index]
            cnv = self.types[index]
            val = a[index + 1]
            try:
                if cnv == "float":
                    obj[key] = float(val)
                elif cnv == "int":
                    obj[key] = int(val)
                elif cnv == "degMin":
                    obj[key] = self.__mkDegrees(val)
                elif cnv == "datetime":
                    obj[key] = self.__strptime(val)
                elif cnv == "TRUE":
                    obj[key] = True
                else:
                    raise Exception("Unrecognized conversion type, {}".format(cnv))
            except Exception as e:
                logging.exception("Error converting {} to type {} for {}".format(
                    val, cnv, key))
                raise e
        return True

numPattern = r"([+-]?\d*[.]?\d+|[+-]?\d*[.]?\d+[Ee][+-]?\d+)"

patterns = [
        MyPattern("float", "speed", r"m_avg_speed[(]m/s[)]\s+" + numPattern),
        MyPattern("datetime", "t",
            r"Curr Time:\s+(\w+\s+\w+\s+\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+MT:\s*\d+"),
        MyPattern(("degMin", "degMin", "float"), ("lat", "lon", "dtLatLon"),
            r"GPS\s+Location:\s+" + numPattern + r"\s+[NS]\s+" +
            numPattern + r"\s+[EW]\s+measured\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("degMin", "float"), ("latWpt", "dtLatWpt"),
            r"sensor:c_wpt_lat[(]lat[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("degMin", "float"), ("lonWpt", "dtLonWpt"),
            r"sensor:c_wpt_lon[(]lon[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("float", "float"), ("vx", "dtVx"),
            r"sensor:m_water_vx[(]m/s[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern(("float", "float"), ("vy", "dtVy"),
            r"sensor:m_water_vy[(]m/s[)]=" + numPattern + r"\s+" + numPattern + r"\s+secs ago"),
        MyPattern("TRUE", "FLAG", r"s \*[.](sbd|tbd) \*[.](sbd|tbd)"),
        ] 

class Dialog(dict):
    def __init__(self, logger:logging.Logger):
        dict.__init__(self)
        self.logger = logger
        for item in patterns:
            for key in item.keys:
                self[key] = None

    def __repr__(self) -> str:
        msg = []
        for key in sorted(self):
            msg.append("{}={}".format(key, self[key]))
        return "\n".join(msg)

    def __iadd__(self, line:str):
        line = line.strip()
        for pattern in patterns:
            if pattern.check(line, self, self.logger): break
        return self

    def flagged(self) -> bool:
        key = "FLAG"
        rc = self[key] if (key in self) and (self[key] is not None) else False
        self[key] = None
        return rc

    def glider(self) -> WayPoint.Glider:
        spd = 0.3 if self['speed'] is None else self['speed']
        return WayPoint.Glider(self['lat'], self['lon'], spd)

    def water(self) -> WayPoint.Water:
        return WayPoint.Water(self['vx'], self['vy'])

if __name__ == "__main__":
    import argparse
    import MyLogger

    parser = argparse.ArgumentParser()
    parser.add_argument("fn", nargs="+", metavar="dialog(s)", help="SFMC dialog file(s)")
    MyLogger.addArgs(parser)
    args = parser.parse_args()

    logger = MyLogger.mkLogger(args)

    for fn in args.fn:
        print("Working on", fn)
        dialog = Dialog(logger)
        with open(fn, "rb") as fp:
            for line in fp:
                dialog += line
                if dialog.flagged():
                    logger.info("line=%s", line)
                    logger.info("Processing\n%s", dialog)
        logger.info("DIALOG\n%s", dialog)
