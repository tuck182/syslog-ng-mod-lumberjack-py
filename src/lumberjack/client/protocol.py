from zope.interface import implementer, implements

from twisted.internet import protocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.abstract import _ConsumerMixin

import collections
import struct
import sys
import warnings

from lumberjack.client.encoder import Encoder 

class IncompleteResponse(Exception):
    """
    Not enough data to parse a full response.
    """

class ParseError(Exception):
    """
    The incoming data is not a valid packet.
    """

# FIXME: Have connection (protocol) manage message sequence, since it's only relevant per-connection
class LumberjackProtocol(protocol.Protocol, _ConsumerMixin):
  connected = 0
  disconnected = 0
  disconnecting = 0
  _remainingData = ""
  _pendingMessages = collections.OrderedDict()
  
  def connectionMade(self):
    self.compress_and_write(Encoder.int_frame('W', 0)) # window_size
    try:
      self.factory.producer.addConsumer(self)
    except:
      import traceback
      traceback.print_exc()
      print "addConsumer failed: {0}".format(sys.exc_info()[1])
    self.producer.resumeProducing()
    self._remainingData = b""

  def connectionLost(self, reason):
    self.producer.removeConsumer(self)
    self.sendData = False
    while self._pendingMessages:
      sequence, message = self._pendingMessages.popitem(True)
      try:
        # FIXME: Shouldn't need to pass message
        message.handler.errback(message)
      except:
        print("Got an error '{0}' trying to NAK message {1}".format(sys.exc_info()[1], sequence))

  def write(self, message):
    # FIXME: Need to handle SSL disconnects
    self._pendingMessages[message.sequence] = message
    self.compress_and_write(Encoder.to_frame(message.data, message.sequence))

  def compress_and_write(self, data):
    self.transport.write(Encoder.compress(data))

  def dataReceived(self, data):
    self._remainingData += data
    while self._remainingData:
        try:
            self._consumeData()
        except IncompleteResponse:
            break
          
  def ackReceived(self, sequence):
    try:
      message = self._pendingMessages.pop(sequence)
      # FIXME: Shouldn't need to pass message
      message.handler.callback(message)
    except KeyError:
      warnings.warn("Received ack for unknown message {0}".format(sequence))

  def _consumeData(self):
    if len(self._remainingData) < 6:
      raise IncompleteResponse
    packet, self._remainingData = self._remainingData[0:6], self._remainingData[6:]
    (_, packet_type, sequence) = struct.unpack("!ccI", packet) 
    if packet_type != 'A':
      raise ParseError("Whoa we shouldn't get this frame: {0}".format(packet_type))
    self.ackReceived(sequence)



class LumberjackProtocolFactory(ReconnectingClientFactory):
  protocol = LumberjackProtocol

  def __init__(self, producer):
    self.producer = producer
