from lumberjack.util.__init__ import *

import struct
import zlib

from pprint import pprint

class Encoder(object):
  @classmethod
  def int_frame(cls, code, value):
    return cls.pack("!ccI", '1', code, value)
  
  @classmethod
  def compress(cls, data):
    compressed = zlib.compress(data)
    size = strlen(compressed)
    return Encoder.pack("!ccI{0}s".format(size), '1', 'C', size, compressed)

  @classmethod
  def to_compressed_frame(cls, h, sequence):
    compressed = zlib.compress(cls.to_frame(h, sequence))
    size = strlen(compressed)
    return Encoder.pack("!ccI{0}s".format(size), '1', 'C', size, compressed)

  @classmethod
  def pack(cls, fmt, *data):
    try:
      result = struct.pack(fmt, *data)
      return result
    except struct.error as e:
      print "struct.pack failed using fmt '{0}'".format(fmt)
      print("for data:")
      pprint(data)
      raise e

  @classmethod
  def to_frame(cls, h, sequence):
    if sequence is None:
      sequence = 0

    fmt = "!ccI"
    data = ['1', 'D', sequence]
    
    flattened = flatten(h)
    
    fmt += "I"
    data.append(len(flattened))
    
    for k, v in flattened.iteritems():
      if v is None:
        v = ""
      if isinstance(v, unicode):
        v = v.encode('latin_1')
      key_length = strlen(k)
      val_length = strlen(v)
      
      fmt += "I"
      data.append(key_length)
      
      if key_length > 0:
        fmt += "{0}s".format(key_length)
        data.append(k)
        
      fmt += "I"
      data.append(val_length)
      
      if val_length > 0:
        fmt += "{0}s".format(val_length)
        data.append(v)
      
    return Encoder.pack(fmt, *data)

