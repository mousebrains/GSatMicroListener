#! /usr/bin/env python3
#
# Load a YAML file containing the pattern
#
# July-2020, Pat Welch, pat@mousebrains.com

import math
import yaml
from WayPoint import Pattern

class Patterns(dict):
    """ A dictionary of Pattern objects """
    def __init__(self, fn:str) -> None:
        dict.__init__(self)
        try:
            with open(fn, 'r') as fp:
                data = yaml.safe_load(fp)
                for glider in data:
                    print(glider, data[glider])
                    data[glider]["patterns"] = self.__toPattern(data[glider])
                self.update(data)
        except Exception as e:
            print("Error opening", fn)
            raise e

    @staticmethod
    def __transform(info:dict) -> list:
        patterns = info["pattern"] if "pattern" in info else []
        theta = info["theta"] if "theta" in info else None
        norm = info["norm"] if "norm" in info else 1

        if (theta is not None) and (theta != 0):
            theta = math.radians(theta) # Degrees -> radians
            stheta = math.sin(theta)
            ctheta = math.cos(theta)
            items = []
            for item in patterns:
                x = ctheta * item[0] - stheta * item[1]
                y = stheta * item[0] + ctheta * item[1]
                items.append((x, y))
            patterns = items

        if (norm is not None) and (norm != 1):
            items = []
            for item in patterns:
                x = norm * item[0]
                y = norm * item[1]
                items.append((x, y))
            patterns = items

        return patterns

    def __toPattern(self, info:dict) -> list:
        patterns = self.__transform(info)
        qRotate = info['qRotate'] if 'qRotate' in info else False
        items = []
        for pattern in patterns:
            items.append(Pattern(pattern[0], pattern[1], qRotate))
        return items

    def qGlider(self, glider:str) -> bool:
        return glider in self

    def qEnabled(self, glider:str) -> bool:
        if glider not in self: return False
        if "enabled" not in self[glider]: return True
        return self[glider]["enabled"]

    def pattern(self, glider:str) -> list:
        if glider not in self: return []
        if "patterns" not in self[glider]: return []
        return self[glider]["patterns"]

    def IMEI(self, glider:str) -> str:
        if glider not in self: return None
        if "IMEI" not in self[glider]: return None
        return self[glider]["IMEI"]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("fn", nargs="+", metavar="fn.yaml", help="YAML file(s)")
    args = parser.parse_args()

    for fn in args.fn:
        a = Patterns(fn)
        for gld in sorted(a):
            print(gld, a[gld])
