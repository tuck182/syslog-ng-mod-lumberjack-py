from lumberjack import *
from lumberjack.connection import Connection
from lumberjack.message_queue import MessageQueue
from lumberjack.util import *

import collections
import random
import socket
import time
import traceback
from multiprocessing import Process, Queue

class Client(object):
  _queue = None
  _shutdown = False
  _shutdown_graceful = None
  _connection = None
  
  def __init__(self, **kwargs):
    print "Client: creating thread"
    self._queue = MessageQueue(Queue())
    self._thread = Process(
      target = self, 
      name = "lumberjack.Connection",
      kwargs = kwargs
      )
    print "Client: starting thread"
    self._thread.start()

  def __call__(self, *args, **kwargs):
    print "Client: thread started"
    self._connection = Connection(queue = self._queue, **kwargs)
    while True and not self._shutdown:
      (id, message) = self._queue.pop()
      self._connection.send(message, id)
    
    while self._shutdown_graceful:
      try:
        (id, message) = self._queue.pop(block = False)
        self._connection.send(message, id)
      except Queue.Empty:
        break
    self._connection.shutdown(self._shutdown_graceful)
    self._connection = None

  def write(self, message):
    self._queue.add(message)

  def shutdown(self, graceful = True):
    self._shutdown_graceful = graceful
    self._shutdown = True
    self._thread.join()

  def acknowledge_sent(self, message_id):
    self._queue.acknowledge(message_id)
