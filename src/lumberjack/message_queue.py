import collections
import uuid
import threading
from Queue import *

from pprint import pprint

SEQUENCE_MAX = (2**32-1)

class MessageQueue(object):
  _messages_queue = None
  _retry_queue = collections.OrderedDict()
  _pending_messages = collections.OrderedDict()
  _sequence = 0
  
  def __init__(self, messages_queue):
    self._messages_queue = messages_queue
    pass

  def add(self, message):
    self._messages_queue.put((self._inc(), message), block = False)

  def pop(self, block = True):
    if self._retry_queue:
      (id, message) = self._retry_queue.popitem(last = False)
    else:
      (id, message) = self._messages_queue.get(block)
    self._pending_messages[id] = message
    return (id, message)

  def acknowledge(self, id):
    del self._pending_messages[id]
    
  def nack_all(self):
    (self._pending_messages, self._retry_queue) = (self._retry_queue, self._pending_messages)
    self._retry_queue.update(self._pending_messages)
    self._pending_messages.clear()

  def _inc(self):
    self._sequence = self._sequence + 1
    if self._sequence > SEQUENCE_MAX:
      self._sequence = 1
    return self._sequence
