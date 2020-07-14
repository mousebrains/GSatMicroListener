# 
# Parse Mobile Originated Message from GSatMicro
#
# https://en.wikipedia.org/wiki/Draft:GSE_Open_GPS_Protocol
#
# July-2020, Pat Welch, pat@mousebrains.com

import datetime as dt
import logging

class BitArray:
    def __init__(self, payload:bytes) -> None:
        self.payload = ""
        for c in payload:
            a = "{:08b}".format(c) # Convert to binary string, 0b01010101
            self.payload += a # Append to the payload string

    def getInt(self, offset:int, nBits:int) -> int:
        return int(self.payload[offset:(offset+nBits)], 2)

    def getFloat(self, offset:int, nBits:int) -> float:
        return float(self.getInt(offset, nBits))

class Message(dict):
    """ A Mobile Originated message decoded """
    def __init__(self, msg:bytes, logger:logging.Logger) -> None:
        super().__init__(self) # Initialize dict
        self.logger = logger
        self.__parse(msg)

    def __parse(self, msg:bytes) -> None:
        """ Parse a mobile originated packet 
        DirectIP SBD message parsing
        See the "Iridium Short Burst Data Service Developers GUide, pages 16+
        https://usermanual.wiki/Pdf/Iridium20Short20Burst20Data20Service20Developers20Guide20v30.896763731/html
        https://en.wikipedia.org/wiki/Draft:GSE_Open_GPS_Protocol
        """

        if len(msg) == 0: return

        version = msg[0]
        if version != 1: # I only know how to parse version 1 messages
            self.logger.error("Invalid message version byte, %s != 1, %s", version, msg)
            return
        
        n = int.from_bytes(msg[1:3], "big")
        payload = msg[3:]
        if len(payload) != n:
            self.logger.error("Invalid message length %s != %s, %s", n, len(payload), msg)
            return
        
        # Now process the MO Information Elements, see sectioin 6.2.4
        while len(payload): # Walk through the information elements
            hdr = payload[0] # See table 6.3
            n = int.from_bytes(payload[1:3], "big")
            if hdr == 1: self.__header(payload, n)
            elif hdr == 2: self.__payload(payload, n)
            elif hdr == 3: self.__location(payload, n)
            elif hdr == 4: self.__confirmation(payload, n)
            else:
                self.logger.error("Unrecognized MO Header IEI, %s, payload %s, msg %s", hdr, payload, msg)
                return
            payload = payload[(3 + n):]
    
    def __header(self, msg:bytes, n:int) -> None:
        """ See table 6.4 """
        if n != 28:
            self.logger.error("Invalid MO IE Header length, %s, in %s", n, msg)
            return
        self['cdr'] = int.from_bytes(msg[4:8], "big")
        self['IMEI'] = str(msg[7:22], 'utf-8')
        self['statSession'] = msg[22]
        self['MOMSN'] = int.from_bytes(msg[23:25], "big")
        self['MTMSN'] = int.from_bytes(msg[25:27], "big")
        t = int.from_bytes(msg[27:31], "big")
        self['tSession'] = dt.datetime.fromtimestamp(t, tz=dt.timezone.utc)

    def __location(self, msg: bytes, n:int) -> None:
        """ See section 6.2.6 """
        if n != 11:
            self.logger.error("Invalid MO IE Location length, %s, in %s", n, msg)
            return
        resvBits = (msg[3] & 0xf0) >> 4 # Should always be zero
        fmtBits = (msg[3] & 0x0c) >> 2 # Should always be zero
        nsBit = (msg[3] & 0x02) >> 1 # North=0 South=1
        ewBit = msg[3] & 0x01 # East=0 West=1
        
        if resvBits != 0:
            self.logger.error("Invalid MO IE reserved bits, %s, in %s", resvBits, msg)
            return
        if fmtBits != 0:
            self.logger.error("Invalid MO IE format code, %s, in %s", fmtBits, msg)
            return

        latDeg = msg[4]
        latMin = int.from_bytes(msg[5:7], "big")
        lonDeg = msg[7]
        lonMin = int.from_bytes(msg[8:10], "big")
        lat = latDeg + latMin / 1000 / 60
        lon = lonDeg + lonMin / 1000 / 60
        if nsBit: lat = -lat
        if ewBit: lon = -lon

        self['latitudeMO'] = lat
        self['longitudeMO'] = lon
        self['radiusMO'] = int.from_bytes(msg[10:14], "big") * 1000 # km->m

    def __confirmation(self, msg:bytes, n:int) -> None:
        if n != 1:
            self.logger.error("Invalid MO IE Confirmation length, %s, in %s", n, msg)
            return
        self['confirmation'] = msg[3]

    def __payload(self, msg:bytes, n:int) -> None:
        """ See section 6.2.5 and
        https://en.wikipedia.org/wiki/Draft:GSE_Open_GPS_Protocol
        """
        if n > len(msg[3:]):
            self.logger.error("MO payload is too short, %s < %s, in %s", len(msg[3:]), n, msg)
            return
        
        payload = msg[3:(n+3)] # Skip IE 1 byte header and 2 byte length
        hdr = payload[0] # Message block type
        body = payload[1:]
        if   hdr == 0: 
            self.__gpsReserved(body)
        elif hdr == 4: 
            self.__gps10Byte(body)
        elif hdr == 5: 
            self.__gps18Byte(body)
        else:
            self.logger.error("Unsupported MO payload type, %s, in %s", hdr, msg)
            return
        self['payload'] = body
    
    def __gpsReserved(self, msg:bytes) -> None:
        pass # For now a noop

    def __gps10Byte(self, msg: bytes) -> None:
        """ NOTE: this has not been fully tested!!!!!!!! 
        See:
        https://www.gsatmicro.com/support/wiki/gsatmicro-wiki#Mobile_Originated_Position_Format_35
        https://docs.google.com/spreadsheets/d/1zKk7TwI3MrkcOo3rwai4zFwZRb1aOcmdsN4IQGLEViM/edit#gid=0
        """
        bits = BitArray(msg)
        magic = bits.getInt(0, 3) # Not specified
        self['longitude'] = bits.getFloat(3, 23) / 23301 - 180
        self['heading'] = bits.getInt(26, 6) * 5
        dt = dt.timedelta(minutes=bits.getInt(32, 10)*2) # n*(2 minutes since midnight)
        midnight = dt.datetime.combine(self['tSession'].date(), dt.time(0,0,0), dt.timezone.utc)
        if dt > (self['tSession'] - midnight): # Wrapped
            midnight -= dt.timedelta(days=1)
        self['t'] = midnight + dt.timedelta(minutes=2*dt)
        self['latitude'] = bits.getFloat(42, 22) / 23301 - 90
        self['speed'] = bits.getInt(64, 6)
        self['altitude'] = bits.getInt(70, 10) * 5 

    def __gps18Byte(self, msg: bytes) -> None:
        """ See
        https://www.gsatmicro.com/support/wiki/gsatmicro-wiki#Mobile_Originated_Position_Format_35
        https://docs.google.com/spreadsheets/d/1hul-GmAiQQc3RCVPAT5al8BFWF549DuAEvkMChY-1I4/edit#gid=0
        and
        https://github.com/darren1713/GSatMicroPublic/blob/master/GSatMicroLibrary/GSatMicroPosition.cs
        """
        bits = BitArray(msg)

        magic = bits.getInt(0, 3)
        if magic != 0:
            self.logger.error("Invalid magic in 18Byte message, %s, %s", magic, msg)
            return

        self['longitude'] = bits.getFloat(3, 26) / 186413 - 180
        self['extPwr'] = bits.getInt(29, 1) != 0
        self['qDistress'] = bits.getInt(30, 1) != 0
        self['qCheckin'] = bits.getInt(31, 1) != 0
        secs = bits.getInt(32, 29)
        self['t'] = dt.datetime(2015,1,1,0,0,0, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=secs)
        self['nSats'] = bits.getInt(61,3)
        self['latitude'] = bits.getFloat(64, 25) / 186413 - 90
        self['heading'] = bits.getInt(89, 6) * 5
        self['accuracy'] = bits.getInt(95, 6)
        # self['climbRate'] = (bits.getFloat(101, 11) - math.pow(2, 10)) / 20
        self['climbRate'] = bits.getInt(101, 11) # Always zero?
        self['battery'] = bits.getInt(112, 5) * 3
        self['speed'] = bits.getInt(117, 11)
        self['altitude'] = bits.getInt(128, 16)

    def qSave(self) -> bool:
        if 'IMEI' not in self: return False
        if 't' not in self:
            if 'tSession' not in self: return False
            self['t'] = self['tSession']
        return True
