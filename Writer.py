#
# Write a Mobile originated message to a database
#
# Feb-2020, Pat Welch, pat@mousebrains.com

import queue
import argparse
import logging
import sqlite3
from datetime import datetime
from ParseMessage import Message
from MyBaseThread import MyBaseThread

class Raw:
    def __init__(self, tbl:str, logger:logging.Logger) -> None:
        self.tbl = tbl
        self.logger = logger

    def createTable(self, cur:sqlite3.Cursor) -> None:
        sql = "CREATE TABLE IF NOT EXISTS " + self.tbl + "( -- GSatMicro DirectIP packets\n"
        sql+= "    t DATETIME WITH TIME ZONE PRIMARY KEY, -- timestamp when connection was made\n"
        sql+= "    addr TEXT, --IP address connection was from\n"
        sql+= "    port INTEGER, -- port number connection was from\n"
        sql+= "    body BLOB -- Binary message\n"
        sql+= ");"
        cur.execute(sql)

    def insert(self, cur:sqlite3.Cursor, t:datetime, addr:str, port:int, msg:bytes) -> None:
        sql = "INSERT OR REPLACE INTO " + self.tbl + " VALUES(?,?,?,?);"
        cur.execute(sql, (t, addr, port, msg))

class MOM:
    """ Mobile Originated Message """
    def __init__(self, tbl:str, logger:logging.Logger) -> None:
        self.tbl = tbl
        self.logger = logger
        self.cols = set()

    def createTable(self, cur:sqlite3.Cursor) -> None:
        sql = "CREATE TABLE IF NOT EXISTS " + self.tbl + "( -- GSatMicro MOM contents\n"
        sql+= "    IMEI TEXT, -- GPS IMEI number\n"
        sql+= "    tRecv TIMESTAMP WITH TIME ZONE, -- When msg was received\n"
        sql+= "    -- Mobile Originated Message, MOM, header fields\n"
        sql+= "    cdr BIGINT DEFAULT NULL, -- packet sequence number\n"
        sql+= "    statSession INTEGER DEFAULT NULL, -- sessionstatus\n"
        sql+= "    MOMSN INTEGER DEFAULT NULL, -- MOM sequence number\n"
        sql+= "    tSession TIMESTAMP WITH TIME ZONE, -- Time MOM was sent\n"
        sql+= "    -- MOM location fields\n"
        sql+= "    latitudeMO DOUBLE PRECISION, -- latitude from Iridium Satellites\n"
        sql+= "    longitudeMO DOUBLE PRECISION, -- longitude from Iridium Satellites\n"
        sql+= "    radiusMO INTEGER, -- radius in meters of lat/lonMO accuracy\n"
        sql+= "    -- MOM payload\n"
        sql+= "    payload BLOB, -- Payload that can be decrypted latter if need be\n"
        sql+= "    t TIMESTAMP WITH TIME ZONE, -- GPS fix time\n"
        sql+= "    latitude DOUBLE PRECISION DEFAULT NULL, -- GPS latitude\n"
        sql+= "    longitude DOUBLE PRECISION DEFAULT NULL, -- GPS longitude\n"
        sql+= "    accuracy FLOAT DEFAULT NULL, -- Accuracy in meters of GPS fix\n"
        sql+= "    altitude FLOAT DEFAULT NULL, -- altitude of fix in meters\n"
        sql+= "    battery FLOAT DEFAULT NULL, -- battery percentage\n"
        sql+= "    climbRate FLOAT DEFAULT NULL, -- climb rate in meters/sec\n"
        sql+= "    heading FLOAT DEFAULT NULL, -- heading in degrees\n"
        sql+= "    speed FLOAT DEFAULT NULL, -- Speed in meters/sec\n"
        sql+= "    nSats INTEGER DEFAULT NULL, -- Number of satellites used for fix\n"
        sql+= "    extPwr BOOLEAN DEFAULT NULL, -- is the device powered externally?\n"
        sql+= "    qCheckin BOOLEAN DEFAULT NULL, -- Is this a checkin?\n"
        sql+= "    qDistress BOOLEAN DEFAULT NULL, -- Is the device in distress?\n"
        sql+= "    PRIMARY KEY(t,IMEI) -- One fix at time t per IMEI\n"
        sql+= ");\n"
        cur.execute(sql)

        # Now get the columns into self.cols
        self.cols = set()
        sql = "PRAGMA table_info(" + self.tbl + ");"
        for row in cur.execute(sql):
            self.cols.add(row[1])

    def insert(self, cur:sqlite3.Cursor, t:datetime, addr:str, port:int, msg:bytes) -> None:
        a = Message(msg, self.logger)
        if not a.qSave(): return
        names = ['tRecv']
        vals = [t]
        for key in a:
            if key in self.cols:
                names.append(key)
                vals.append(a[key])
        sql = "INSERT OR REPLACE INTO " + self.tbl
        sql+= "(" + ",".join(names) + ")"
        sql+= " VALUES(" + ",".join(["?"] * len(names)) + ");"
        cur.execute(sql, vals)

class Writer(MyBaseThread):
    ''' Wait on a queue, and write the item to a file '''
    def __init__(self, args:argparse.ArgumentParser, logger:logging.Logger) -> None:
        MyBaseThread.__init__(self, "Writer", args, logger)
        self.dbName = args.db
        self.raw = Raw(args.raw, logger)
        self.mom = MOM(args.mom, logger)
        self.q = queue.Queue()

    @staticmethod
    def addArgs(parser:argparse.ArgumentParser) -> None:
        grp = parser.add_argument_group(description="Database writer options")
        grp.add_argument("--db", type=str, required=True, metavar='filename',
                help="SQLite3 database filename")
        grp.add_argument("--raw", type=str, default="Raw", metavar='name',
                help="Table name for raw information")
        grp.add_argument("--mom", type=str, default="MOM", metavar='name',
                help="Table name for Mobile Originated Messages")

    def runAndCatch(self) -> None:
        '''Called on thread start '''
        self.logger.debug("Creating tables")
        try:
            with sqlite3.connect(self.dbName) as conn:
                cur = conn.cursor()
                self.raw.createTable(cur)
                self.mom.createTable(cur)
                conn.commit()
        except:
            self.logger.exception("Error creating tables in %s", self.dbName)

        while True: # Loop forever
            (t, addr, msg) = self.q.get()
            try:
                self.logger.info('t=%s addr=%s:%s msg=%s', t, addr[0], addr[1], msg)
                with sqlite3.connect(self.dbName) as conn:
                    self.logger.info('%s', msg)
                    cur = conn.cursor()
                    self.raw.insert(cur, t, addr[0], addr[1], msg)
                    self.mom.insert(cur, t, addr[0], addr[1], msg)
                    conn.commit()
            except:
                self.logger.exception('Exception while writing to %s', self.dbName)
            self.q.task_done()
