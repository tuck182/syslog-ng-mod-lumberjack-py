from lumberjack.client.process import ClientProcess

from pprint import pprint
import sys

class Client(object):
  _process = None
  
  def __init__(self, **kwargs):
    self._process = ClientProcess(**kwargs)
    self._process.start()

  def write(self, message):
    self._process.write(message)

  def shutdown(self, graceful = True):
    self._process.shutdown(graceful)
