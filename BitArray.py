#
# Bit manipulation
#
# July-2020, Pat Welch, pat@mousebrains.com

import math

class BitArray:
    def __init__(self, payload:bytes) -> None:
        self.payload = ""
        if len(payload) > 0:
            fmt = self.__mkFormat(len(payload) * 8)
            self.payload += fmt.format(int.from_bytes(payload, "big"))

    def __repr__(self) -> str:
        hdr = ""
        for i in range(len(self.payload)):
            hdr += str(i % 10)
        return hdr + "\n" + self.payload

    def to_bytes(self) -> bytes:
        nBytes = math.ceil(len(self.payload) / 8)
        return int(self.payload, 2).to_bytes(nBytes, "big")

    @staticmethod
    def __limitValue(nBits:int, val:int) -> int:
        upperLimit = 2**nBits
        if (val >= 0) and (val < upperLimit): return val
        if val < 0:
            print("Value is negative, {}, setting to zero", val)
            return 0

        print("Value too big, {}, limit is {}, capping", val, upperLimit-1)
        return upperLimit -1

    @staticmethod
    def __mkFormat(nBits:int) -> str:
        return "{:0" + str(nBits) + "b}"

    def append(self, nBits:int, val:int) -> None:
        val = self.__limitValue(nBits, val)
        fmt = self.__mkFormat(nBits)
        self.payload += fmt.format(val)

    def set(self, offset:int, nBits:int, val:int) -> None:
        val = self.__limitValue(nBits, val)
        fmt = self.__mkFormat(nBits)
        self.payload[offset:(offset+nBits)] = fmt.format(val)

    def get(self, offset:int, nBits:int) -> str:
        return self.payload[offset:(offset+nBits)]

    def getInt(self, offset:int, nBits:int) -> int:
        return int(self.get(offset, nBits), 2)

    def getFloat(self, offset:int, nBits:int) -> float:
        return float(self.getInt(offset, nBits))

