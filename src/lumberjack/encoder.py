from lumberjack.util import *

import struct
import zlib

class Encoder(object):
  @classmethod
  def to_compressed_frame(cls, hash, sequence):
    compressed = zlib.compress(cls.to_frame(hash, sequence))
    size = strlen(compressed)
    return Encoder.pack("!ccI{0}s".format(size), '1', 'C', size, compressed)

  @classmethod
  def pack(cls, format, *data):
    try:
      result = struct.pack(format, *data)
      return result
    except struct.error as e:
      print "struct.pack failed using format '{0}'".format(format)
      print("for data:")
      pprint(data)
      raise e

  @classmethod
  def to_frame(cls, hash, sequence):
    if sequence is None:
      sequence = 0
      
    format = "!ccI"
    data = ['1', 'D', sequence]
    
    flattened = flatten(hash)
    
    format += "I"
    data.append(len(flattened))
    
    for k, v in flattened.iteritems():
      if v is None:
        v = ""
      if isinstance(v, unicode):
        v = v.encode('latin_1')
      key_length = strlen(k)
      val_length = strlen(v)
      
      format += "I"
      data.append(key_length)
      
      if key_length > 0:
        format += "{0}s".format(key_length)
        data.append(k)
        
      format += "I"
      data.append(val_length)
      
      if val_length > 0:
        format += "{0}s".format(val_length)
        data.append(v)
      
    return Encoder.pack(format, *data)

