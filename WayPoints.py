#! /usr/bin/env python3
#
# Create a set of waypoints for a glider
#
# July-2020, Pat Welch, pat@mousebrains.com

import argparse
import logging
import WayPoint
import copy
import datetime
import math
import sqlite3
from geopy.distance import distance as geodesic

class WayPoints(list):
    def __init__(self, 
            drifter:WayPoint.Drifter, 
            glider:WayPoint.Glider,
            water:WayPoint.Water,
            patterns:list,
            args:argparse.ArgumentParser,
            logger:logging.Logger,
            index:int = 0) -> None:
        list.__init__(self)
        self.drifter = drifter
        self.glider = glider
        self.water = water
        self.patterns = patterns
        self.args = args
        self.logger = logger
        self.index = index

        dt = 0
        drft = copy.deepcopy(drifter)
        gld  = copy.deepcopy(glider)
        if index is None: # Start with closest in time
            index = self.__findClosest(drifter, glider, water, patterns)
            self.index = index

        while ((dt <= args.wptsTgtDuration) or (len(self) < 2)) \
                and (len(self) < args.wptsCount):
            if index >= len(patterns):
                index = 0
            wpt = WayPoint.WayPoint(drft, gld, water, patterns[index])
            dt += wpt.dt
            self.append((wpt, dt, index))
            drft = wpt.drifter1
            gld = wpt.glider1
            index += 1

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="WayPoints options")
        grp.add_argument("--wptsMinDuration", type=int, default=4*3600,
                help="Minimum duration a waypoint plan can be for in seconds")
        grp.add_argument("--wptsTgtDuration", type=int, default=24*3600,
                help="Target duration a waypoint plan can be for in seconds")
        grp.add_argument("--wptsCount", type=int, default=7,
                help="Maximum number of waypoints to generate")
        grp.add_argument("--wptsDB", type=str, metavar="foo.db",
                help="Filename of database for historical waypoints")
        grp.add_argument("--wptsMatchRadius", type=float, default=100, metavar="meters",
                help="Radius in meters to consider a match")

    def __findClosest(self, 
            drifter:WayPoint.Drifter, 
            glider:WayPoint.Glider, 
            water:WayPoint.Water, 
            patterns:list) -> int:
        """ Find pattern index which is closest in time """
        tMin = None
        index = None
        for i in range(len(patterns)):
            wpt = WayPoint.WayPoint(drifter, glider, water, patterns[i])
            print(wpt.dt, i, tMin, index)
            if tMin is None:
                tMin = wpt.dt
                index = i
            elif tMin > wpt.dt:
                tMin = wpt.dt
                index = i
        return index

    def __printDrifter(self, d:WayPoint.Drifter) -> list:
        return [
                "# DRIFTER pos: {:.6f}, {:.6f}".format(d.latLon.lat, d.latLon.lon),
                "#         vel: {:.4f}, {:.4f} m/sec".format(d.v.x, d.v.y),
                "#       speed: {:.4f} m/sec".format(d.v.speed()),
                "#       theta: {:.1f} degrees true".format(d.v.theta()),
                ]

    def __printGlider(self, g:WayPoint.Glider) -> list:
        return [
                "# GLIDER  pos: {:.6f}, {:.6f}".format(g.latLon.lat, g.latLon.lon),
                "#       speed: {:.4f} m/sec".format(g.speed),
                ]

    def __printWater(self, w:WayPoint.Water) -> list:
        return [
                "# WATER   vel: {:.4f}, {:.4f} m/sec".format(w.v.x, w.v.y),
                "#       speed: {:.4f} m/sec".format(w.v.speed()),
                "#       theta: {:.1f} degrees true".format(w.v.theta()),
                ]

    def __checkForward(self, prev:list) -> float:
        radius = self.args.wptsMatchRadius
        totalTime = 0
        maxDist = 0
        for i in range(min(len(prev), len(self))):
            (wpt, dt, iSelf) = self[i]
            (t, iwpt, lat, lon, index, dist, dt) = prev[i]
            if iSelf != index: 
                return None # Not the same pattern point
            delta = wpt.wpt.distance(WayPoint.LatLon(lat, lon))
            if delta > radius:
                return None # Too far away
            totalTime += dt
            maxDist = max(maxDist, delta)

        return maxDist if totalTime >= self.args.wptsMinDuration else None

    def __closeEnough(self, prev:list) -> float:
        (wpt, dt, iSelf) = self[0]
        for j in range(len(prev)):
            (t, iwpt, lat, lon, index, dist, dt) = prev[j]
            if index == iSelf:
                maxDist = self.__checkForward(prev[j:])
                if maxDist is not None:
                    return maxDist

        return None

    def __qGenGoto(self) -> float:
        args = self.args
        if args.wptsDB is None:
            self.logger.info("No wptsDB")
            return None # No historical data to compare to
        with sqlite3.connect(args.wptsDB) as db:
            sql = "CREATE TABLE IF NOT EXISTS waypoints ( -- Waypoints for goto\n"
            sql+= "    t TIMESTAMP WITH TIMEZONE, -- When record was created\n"
            sql+= "    iWaypoint INTEGER, -- Waypoint index in goto file\n"
            sql+= "    latitude FLOAT, -- decimal degrees\n"
            sql+= "    longitude FLOAT, -- decimal degrees\n"
            sql+= "    iPattern INTEGER, -- pattern index\n"
            sql+= "    distance FLOAT, -- Distance current position or previous waypoint in m\n"
            sql+= "    dt FLOAT, -- Seconds from current position or previous waypoint\n"
            sql+= "    PRIMARY KEY(t, iWaypoint)\n"
            sql+= ");"
            cur = db.cursor()
            cur.execute(sql)

            sql = "SELECT * FROM waypoints"
            sql+= " WHERE t=(SELECT MAX(t) FROM waypoints)"
            sql+= " ORDER BY iWaypoint;"
            cur.execute(sql)
            prev = [];
            for row in cur:
                prev.append(row)

            maxDist = self.__closeEnough(prev) # Check if close enough to not generate new goto
            if maxDist is not None:
                return maxDist

            sql = "INSERT INTO waypoints VALUES(?,?,?,?,?,?,?);"
            prevLatLon = self.glider.latLon
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            for i in range(len(self)):
                (wpt, dt, index) = self[i]
                lat = wpt.wpt.lat
                lon = wpt.wpt.lon
                dist = wpt.wpt.distance(prevLatLon)
                prevLatLon = wpt.wpt
                cur.execute(sql, (now, i, lat, lon, index, dist, dt))

        return None

    def goto(self, t0:datetime.datetime, IMEI:str) -> tuple:
        maxDist = self.__qGenGoto()
        if maxDist is not None:
            self.logger.info("Goto not qGenGoto, maximum distance is %s", maxDist)
            return (None, maxDist)

        msg = []
        msg.append("behavior_name=goto_list")
        msg.append("# Drifter follower")
        msg.append("# Generated: " + str(t0.replace(microsecond=0)))
        msg.append("#")
        msg.append("# IMEI: " + IMEI)
        msg.append("#")
        msg.extend(self.__printDrifter(self.drifter))
        msg.append("#")
        msg.extend(self.__printGlider(self.glider))
        msg.append("#")
        msg.extend(self.__printWater(self.water))
        msg.append("#")
        msg.append("# PATTERNS index={}:".format(self.index))
        for index in range(len(self.patterns)):
            msg.append("#   i={:.1f} {}".format(index, self.patterns[index]))
        msg.append("#")
        msg.append("")
        msg.append("<start:b_arg>")
        msg.append("b_arg: num_legs_to_run(nodim) -2 # Traverse once")
        msg.append("b_arg: start_when(enum) 0 # BAW_IMMEDIATELY")
        msg.append("b_arg: list_stop_when(enum) 7 # BAW_WHEN_WPT_DIST")
        msg.append("b_arg: initial_wpt(enum) 0")
        msg.append("b_arg: num_waypoints(enum) {}".format(len(self)))
        msg.append("<end:b_arg>")
        msg.append("<start:waypoints>")
        prevLatLon = self.glider.latLon
        for item in self:
            (wpt, dt, index) = item
            (lat, lon) = wpt.wpt.goto()
            dist = wpt.wpt.distance(prevLatLon)
            prevLatLon = wpt.wpt
            t = (t0 + datetime.timedelta(seconds=dt)).replace(microsecond=0)
            dt = datetime.timedelta(seconds=math.floor(wpt.dt))
            msg.append("{:6f} {:6f} # i={}, dist={:.0f}m, dt={}, {}".format(
                lon, lat, index, dist, dt, t))
        msg.append("<end:waypoints>")
        return ("\n".join(msg), None)

if __name__ == "__main__":
    import MyLogger

    parser = argparse.ArgumentParser()
    WayPoints.addArgs(parser)
    MyLogger.addArgs(parser)
    args = parser.parse_args()

    logger = MyLogger.mkLogger(args)

    wpts = WayPoints(
            WayPoint.Drifter(44, -124, 0.0, 0.1),
            WayPoint.Glider(44.01, -124, 0.4),
            WayPoint.Water(-0.1, 0.1),
            [
                WayPoint.Pattern(1000, 0, False), 
                WayPoint.Pattern(-1000,0, False),
                WayPoint.Pattern(0, 1000, False), 
                WayPoint.Pattern(0, -1000, False),
                ],
            args,
            logger,
            index=None
            )

    t0 = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=10)
    t0 = t0.replace(microsecond=0)
    (goto, maxDist) = wpts.goto(t0, "0000000000015")
    if goto is None:
        logger.info("No goto, maxDist %s", maxDist)
    else:
        logger.info("Goto\n%s", goto)
