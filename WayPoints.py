#! /usr/bin/env python3
#
# Create a set of waypoints for a glider
#
# July-2020, Pat Welch, pat@mousebrains.com

import WayPoint
import copy
import datetime
import math

class WayPoints(list):
    def __init__(self, 
            drifter:WayPoint.Drifter, 
            glider:WayPoint.Glider,
            water:WayPoint.Water,
            patterns:list,
            index:int = 0,
            maxWayPoints:int = 7,
            maxDuration:float = 12*3600) -> None:
        list.__init__(self)
        self.index = index

        dt = 0
        drft = copy.deepcopy(drifter)
        gld  = copy.deepcopy(glider)
        if index is None: # Start with closest in time
            index = self.__findClosest(drifter, glider, water, patterns)
            self.index = index

        while (dt <= maxDuration) and (len(self) < maxWayPoints):
            if index >= len(patterns):
                index = 0
            wpt = WayPoint.WayPoint(drft, gld, water, patterns[index])
            dt += wpt.dt
            self.append((wpt, dt, index))
            drft = wpt.drifter1
            gld = wpt.glider1
            index += 1

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

    def goto(self, t0:datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)) -> str:
        msg = []
        msg.append("behavior_name=goto_list")
        msg.append("# Drifter follower")
        msg.append("# Generated: " + str(t0.replace(microsecond=0)))
        msg.append("")
        msg.append("<start:b_arg>")
        msg.append("b_arg: num_legs_to_run(nodim) -1")
        msg.append("b_arg: start_when(enum) 0 # BAW_IMMEDIATELY")
        msg.append("b_arg: list_stop_when(enum) 7 # BAW_WHEN_WPT_DIST")
        msg.append("b_arg: initial_wpt(enum) 0")
        msg.append("b_arg: num_waypoints(enum) {}".format(len(self)))
        msg.append("<end:b_arg>")
        msg.append("<start:waypoints>")
        for item in self:
            (wpt, dt, index) = item
            (lat, lon) = wpt.wpt.goto()
            t = (t0 + datetime.timedelta(seconds=dt)).replace(microsecond=0)
            dt = datetime.timedelta(seconds=math.floor(wpt.dt))
            msg.append("{} {} # i={}, dt={} {}".format(lon, lat, index, dt, t))
        msg.append("<end:waypoints>")
        return "\n".join(msg)

if __name__ == "__main__":
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
            index=None
            )
    # for item in wpts:
        # (wpt, dt, index) = item
        # print("dt", dt, wpt.dt, "n", index, "waypoint", wpt.wpt)

    t0 = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=10)
    t0 = t0.replace(microsecond=0)
    print(wpts.goto(t0))
