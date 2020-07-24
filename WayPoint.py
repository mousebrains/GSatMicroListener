#! /usr/bin/env python3
# 
# Find a waypoint for a glider in a drifter's moving reference frame
#
# Let X(t) and V bet the drifter's vector position and velocity
# Let X0 be the drifters position at time t=0
# Let Y(t) and W bet the glider's vector position and velocity
# Let Y0 be the glider's position at time t=0
# Let spd be the glider's scaler horizontal through water speed
# Let theta be the glider's true angle with respect to latitude, with northwards being theta=90
# Let H(theta0 be the glider's true heading vector, (cos(theta), sin(theta))
# Let U bet the ocean's vector current
# Let t be the time from t0, the starting time
#
# The drifter's position at time t is then
# X(t) = X0 + t V
#
# The glider's position at time t is then
# Y(t,theta) = Y0 + t (U + spd H(theta))
#
# We want to find t and theta for
# X(t) = Y(t,theta)
# X0 + t V = Y0 + t(U + spd H(theta))
#
# Now use cos^2+sin^2 = 1 to eliminate the theta term
# (X0-Y0) + t(V-U) = t spd H(theta)
# [(X0-Y0) + t(V-U)] / (t spd) = H(theta)
#
# square each component and sum componets to get cos^2+sin^2 on RHS
# Let ex and ey be unit vectors parallel to lines of latitude and longitude
#
# [(X0-Y0).ex + t (V-U).ex] / (t spd) = cos(theta)
# [(X0-Y0).ey + t (V-U).ey] / (t spd) = sin(theta)
# N.B. the above pair of equations are used to get theta via arctan
#
# Squaring
# [(X0-Y0).ex^2 + t 2 (X0-Y0).ex*(V-U).ex + t^2 (V-U).ex^2]/(t^2 spd^2) = cos(theta)^2
# [(X0-Y0).ey^2 + t 2 (X0-Y0).ey*(V-U).ey + t^2 (V-U).ey^2]/(t^2 spd^2) = sin(theta)^2
#
# Summing these we end up with a quadratic equation in t
#
# t^2 spd^2 = (X0-Y0).ex^2 + t 2 (X0-Y0).ex*(V-U).ex + t^2 (V-U).ex^2
#           + (X0-Y0).ey^2 + t 2 (X0-Y0).ey*(V-U).ey + t^2 (V-U).ey^2
# t^2 spd^2 = |X0-Y0|^2 + t 2 (X0-Y0) dot (V-U) + t^2 |V-U|^2
#
# 0 = t^2 * (|V-U|^2 - spd^2)
#   + t 2 (X0-Y0) dot (V-U)
#   + |V-U|^2
#
# July-2020, Pat Welch, pat@mousebrains.com

import math
import copy
from geopy.distance import geodesic

class Point:
    def __init__(self, x:float, y:float) -> None:
        self.x = x
        self.y = y

    def __repr__(self) -> str:
        return "[{},{}]".format(self.x, self.y)

    def __add__(lhs, rhs):
        a = copy.copy(lhs)
        a.x += rhs.x
        a.y += rhs.y
        return a

    def __sub__(lhs, rhs):
        a = copy.copy(lhs)
        a.x -= rhs.x
        a.y -= rhs.y
        return a

    def __mul__(lhs, rhs:float):
        a = copy.copy(lhs)
        a.x *= rhs
        a.y *= rhs
        return a

    def rotate(self, theta:float):
        a = copy.copy(self)
        if (theta is not None) and (theta != 0):
            ctheta = math.cos(theta)
            stheta = math.sin(theta)
            a.x = self.x * ctheta - self.y * stheta
            a.y = self.x * stheta + self.y * ctheta
        return a

    def dot(lhs, rhs) -> float:
        return lhs.x * rhs.x + lhs.y * rhs.y

class LatLon:
    def __init__(self, lat:float, lon:float) -> None:
        self.lat = lat
        self.lon = lon

    def __repr__(self) -> str:
        return "[{},{}]".format(self.lat, self.lon)

    def __sub__(lhs, rhs):
        a = copy.copy(lhs)
        a.lat -= rhs.lat
        a.lon -= rhs.lon
        return a

    def delta(lhs, rhs) -> Point:
        lat0 = lhs.lat
        lat1 = rhs.lat
        lon0 = lhs.lon
        lon1 = rhs.lon
        latMid = (lat0 + lat1) / 2
        lonMid = (lon0 + lon1) / 2
        # This is an approximation but for small scale reasonably close
        dx = geodesic((latMid, lon0), (latMid, lon1)).meters
        dy = geodesic((lat0, lonMid), (lat1, lonMid)).meters
        if lat0 > lat1: dy = -dy
        if lon0 > lon1: dx = -dx
        return Point(dx, dy)

    def translate(self, dist:Point):
        # Move myself by a given Cartesian distance
        lat = self.lat
        lon = self.lon
        latPerDeg = geodesic((lat-0.5, lon), (lat+0.5, lon)).meters
        lonPerDeg = geodesic((lat, lon-0.5), (lat, lon+0.5)).meters
        a = copy.copy(self)
        a.lat = self.lat + dist.y / latPerDeg
        a.lon = self.lon + dist.x / lonPerDeg
        return a

    @staticmethod
    def degMin(x) -> float:
        a = abs(x)
        deg = math.floor(a)
        minutes = (a % 1) * 60
        val = deg * 100 + minutes
        return val if x >= 0 else -val

    def goto(self) -> str:
        """ Return lat/lon in deg*100+decimal minutes """
        return (self.degMin(self.lat), self.degMin(self.lon))

class Drifter:
    """ Data structure with drifter data in it """
    def __init__(self, lat, lon, vx, vy) -> None:
        self.latLon = LatLon(lat, lon)
        self.v = Point(vx, vy)
        self.theta = math.atan2(vy, vx)

    def __repr__(self) -> str:
        return "DRIFTER: {} v {} theta {}".format(self.latLon, self.v, math.degrees(self.theta))

class Glider:
    """ Data structure with glider data in it """
    def __init__(self, lat, lon, speed) -> None:
        self.latLon = LatLon(lat, lon)
        self.speed = speed

    def __repr__(self) -> str:
        return "GLIDER: {} spd {}".format(self.latLon, self.speed)

class Water:
    """ Data structure with depth averaged current in it """
    def __init__(self, vx, vy) -> None:
        self.v = Point(vx, vy)

    def __repr__(self) -> str:
        return "CURRENT: {}".format(self.v)

class Pattern:
    """ Data structure with pattern offset from center of drifter location """
    def __init__(self, xOffset, yOffset, qRotate) -> None:
        self.offset = Point(xOffset, yOffset)
        self.qRotate = qRotate

    def __repr__(self) -> str:
        return "PATTERN: {} {}".format(self.offset, self.qRotate)

    def rotate(self, theta:float) -> Point:
        return self.offset.rotate(theta) if self.qRotate else self.offset

class WayPoint:
    def __init__(self, drifter:Drifter, glider:Glider, water:Water, pattern:Pattern) -> None:
        """ 
        lat/lon of the drifter
        velocity of the drifter, vx is eastwards, vy is northwards, in m/sec
        lat/lon of glider, horizontal through water speed of the glider in m/sec
        velocity of the depth averaged current, vx is eastwards, vy is northwards, in m/sec
        xOffset and yOffset are the target point relative to the drifter in m
        qRotate, rotate x/yOffset so that xOffset is parallel drifter velocity
        """
        self.drifter = drifter # Drifter's lat/lon and velocity
        self.glider = glider   # Glider's lat/lon and horizontal speed
        self.water = water     # Depth averaged current velocity
        self.pattern = pattern # Offset from drifter

        self.drifter0 = Point(0,0) # Initially drifter is at center of the universe
        self.target0 = self.pattern.rotate(self.drifter.theta) # Initial pattern location
        self.glider0 = self.drifter.latLon.delta(self.glider.latLon) # Where the glider is initially
        self.dt = self.__dt() # Time to intersection
        self.__mkWaypoint() # time to x/y and lat/lon of waypoint

    def __repr__(self) -> str:
        msg = [str(self.drifter), str(self.glider), str(self.water), str(self.pattern)]
        msg.append("DRIFTER0 {}".format(self.drifter0))
        msg.append(str(self.drifter1))
        msg.append("TARGET0 {}".format(self.target0))
        msg.append("GLIDER0: {}".format(self.glider0))
        msg.append(str(self.glider1))
        msg.append("WYPT: t {} wpt {}".format(self.dt, self.wptXY))
        msg.append("WYPT: {}".format(self.wpt))
        return "\n".join(msg)

    def __dt(self) -> float:
        """ Solve the quadratic equation for time """
        d0 = self.target0 - self.glider0 # initial distance from initial target to glider
        dv = self.drifter.v - self.water.v # drifter minus current velocity
        spd2 = self.glider.speed * self.glider.speed # Speed squared

        # 0 = t^2 (|V-U|^2 - speed^2) + 2 t (X-Y)dot(V-U) + |X-Y|^2
        a = dv.dot(dv) - spd2 # |V-U|^2 - spd^2
        b = 2 * d0.dot(dv) # 2 ((X0-Y0) dot (V-U))
        c = d0.dot(d0) # |X0-Y0|^2

        if a == 0:
            raise Exception("No valid solution, water+drifter speed equals glider speed")

        term = b * b - 4 * a * c

        if term < 0:
            raise Exception("No valid solution, square root term is negative")

        term = math.sqrt(term)

        tp = (-b + term) / (2 * a)
        tm = (-b - term) / (2 * a)

        if tp < 0:
            if tm < 0:
                raise Exception("No valid future solution found")
            return tm
        elif tm < 0:
            return tp
        else:
            return min(tm, tp)

    def __mkWaypoint(self) -> None:
        """ Given a time to waypoint, calculate where the waypoint is """
        dt = self.dt # Duration
        delta = self.drifter.v * dt # Distance in x and y the drifter has moved
        self.drifter1 = self.drifter0 + delta # Where the drifter moved to
        self.wptXY = self.target0 + delta # Where the target moved to
        self.wpt = self.drifter.latLon.translate(delta + (self.target0 - self.drifter0))
        self.glider1 = Glider(self.wpt.lat, self.wpt.lon, self.glider.speed)
        drifter = self.drifter.latLon.translate(delta)
        self.drifter1 = Drifter(drifter.lat, drifter.lon, self.drifter.v.x, self.drifter.v.y)

if __name__ == "__main__":
    wpt = WayPoint(
            Drifter(44, -124, 0.1, 0),
            Glider(44.01, -124, 0.4),
            Water(-0.1, 0.1),
            Pattern(1, 0, True))
    print(wpt)
