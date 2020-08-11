#
# A base thread that catches errors and logs them
#

from threading import Thread
from logging import Logger
from argparse import ArgumentParser

class MyBaseThread(Thread):
    def __init__(self, name:str, args:ArgumentParser, logger:Logger):
        Thread.__init__(self, daemon=True)
        self.name = name
        self.args = args
        self.logger = logger

    def run(self): # Called on thread start
        try:
            self.runAndCatch()
        except:
            self.logger.exception("Unexpected exception")
