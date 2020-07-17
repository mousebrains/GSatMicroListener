#! /usr/bin/env python3
#
# Estimate a current position and velocity vector from a set of GPS points
#
# July-2020, Pat Welch, pat@mousebrains.com

import argparse
import logging
import sqlite3
import MyLogger
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

def distance(lat0, lon0, lat1, lon1):
    """ Geodesic distance in meters on the earth's surface using haversine """
    (lat0, lon0, lat1, lon1) = np.radians((lat0, lon0, lat1, lon1))
    a = \
            np.sin((lat1 - lat0) / 2.)**2 + \
            np.cos(lat0) * np.cos(lat1) * np.sin((lon1 - lon0) / 2.)**2
    earthRadius = 6371 * 1000 
    return earthRadius * 2 * np.arcsin(np.sqrt(a))

class Drifter:
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        self.args = args
        self.logger = logger
        self.sql = "SELECT t,latitude, longitude,accuracy FROM mom"
        self.vals = []
        if args.tEarliest is not None:
            self.sql+= " WHERE t>=?"
            self.vals.append(args.tEarliest)
        self.sql+= " ORDER BY t desc limit ?"
        self.vals.append(args.nBack)

    @staticmethod
    def mkTime(s): # For use in addArgs
        print("mkTime", s)
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except:
            pass
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except:
            pass
        return None

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Drifter options")
        grp.add_argument("--db", type=str, required=True, metavar="filename",
                help="Drifter database name")
        grp.add_argument("--nBack", type=int, default=10, metavar="count",
                help="Number of samples in the past to use in calculation")
        grp.add_argument("--tau", type=float, default=60, metavar="minutes",
                help="expoential downweighting factor for estimates in minutes")
        grp.add_argument("--tEarliest", type=Drifter.mkTime, metavar="timestamp",
                help="Earliest time to fetch")

    def __fetch(self) -> tuple:
        with sqlite3.connect(self.args.db) as conn:
            cur = conn.cursor()
            cur.execute(self.sql, self.vals)
            data = None
            tMax = None
            for row in cur:
                t = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S+00:00")
                t = t.replace(tzinfo=timezone.utc)
                a = [t, row[1], row[2], row[3]]
                b = pd.DataFrame(a, index=['t', 'lat', 'lon', 'accuracy']).transpose()
                b['t'] = b['t'].astype('datetime64[ns]')
                b['lat'] = b['lat'].astype('float64')
                b['lon'] = b['lon'].astype('float64')
                b['accuracy'] = b['accuracy'].astype('float64')
                if data is None:
                    data = b
                    tMax = t
                else:
                    data = data.append(b, ignore_index=True)
            return (tMax, data)

    def estimate(self, t:datetime) -> pd.DataFrame:
        """ Do a weighted linear regression on recent fixes
            then estimate the velocity and the position at time t
            """
        (tMax, data) = self.__fetch() # Fetch rows from database
        tt = (t - tMax).total_seconds()
        data['dt'] = (data['t'] - max(data['t'])).dt.total_seconds()
        data['latPerDeg'] = distance(data['lat'] - 0.5, data['lon'], data['lat'] + 0.5, data['lon'])
        data['lonPerDeg'] = distance(data['lat'], data['lon'] - 0.5, data['lat'], data['lon'] + 0.5)
        data['accLat'] = data['accuracy'] / data['latPerDeg'] 
        data['accLon'] = data['accuracy'] / data['lonPerDeg'] 
        data['wghtLat'] = 1 / (data['accLat'] ** 2)
        data['wghtLon'] = 1 / (data['accLon'] ** 2)
        data['wghtLat'] = data['wghtLat'] / max(data['wghtLat'])
        data['wghtLon'] = data['wghtLon'] / max(data['wghtLon'])
        data['wghtDT'] = np.exp(data['dt'] / (self.args.tau * 60))
        data['weightLat'] = data['wghtLat'] * data['wghtDT']
        data['weightLon'] = data['wghtLon'] * data['wghtDT']
        data['weightLat'] = data['weightLat'] / max(data['weightLat'])
        data['weightLon'] = data['weightLon'] / max(data['weightLon'])
        dt = data['dt'].to_frame()
        lm = LinearRegression()
        lm.fit(dt, data['lat'], data['weightLat'])
        info = pd.DataFrame(lm.predict(tt), columns=["lat"])
        info['vy'] = distance(
                data['lat'][0], data['lon'][0], 
                data['lat'][0] + lm.coef_, data['lon'][0])
        data['latP'] = lm.predict(dt)
        lm.fit(dt, data['lon'], data['weightLon'])
        info['lon'] = lm.predict(tt)
        info['vx'] = distance(
                data['lat'][0], data['lon'][0], 
                data['lat'][0], data['lon'][0] + lm.coef_)
        data['lonP'] = lm.predict(dt)
        return info

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Drifter calculation from GPS fixes")
    Drifter.addArgs(parser)
    MyLogger.addArgs(parser)
    args = parser.parse_args()
    print(args)
    logger = MyLogger.mkLogger(args)
    drifter = Drifter(args, logger)

    info = drifter.estimate(datetime.now(tz=timezone.utc))
    print(info)
