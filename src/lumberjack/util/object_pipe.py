import os
import socket
from pickle import dumps, HIGHEST_PROTOCOL

USE_PIPE = True

class ObjectPipe:
  """
  Provides a one-way socket connection for passing objects across, for IPC.
  """
  def __init__(self):
    if USE_PIPE:
      pipein, pipeout = os.pipe()
      self._reader = os.fdopen(pipein, 'r')
      self._writer = os.fdopen(pipeout, 'w')
    else:
      s1, s2 = socket.socketpair()
      self._reader = os.fdopen(os.dup(s1.fileno()), 'r')
      self._writer = os.fdopen(os.dup(s2.fileno()), 'w')
      s1.close()
      s2.close()

  def get_reader(self):
    return self._reader
  
  def get_writer(self):
    return self._writer
  
  def write(self, obj):
    enc = self._encode(obj)
    self._writer.write(enc)
    self._writer.flush()
    
  def read(self, obj):
    raise NotImplementedError()
    
  def close_reader(self):
    self._reader.close()
    
  def close_writer(self):
    self._writer.close()
    
  def _encode(self, obj):
    data = dumps(obj, HIGHEST_PROTOCOL)
    return b'%d:%s,' % (len(data), data)
