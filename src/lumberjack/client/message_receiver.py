from zope.interface import implementer, implements

from twisted.internet import interfaces
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import NetstringReceiver

from pickle import loads

from lumberjack.message import LumberjackMessage
from lumberjack.util.sequence import SequenceGenerator

class MessageReceiver(NetstringReceiver):
  implements(interfaces.IPushProducer)

  sequence = SequenceGenerator()
  
  def makeConnection(self, transport):
    NetstringReceiver.makeConnection(self, transport)

  def addConsumer(self, consumer):
    consumer.registerProducer(self, True)
    self.consumer = consumer
    
  def stringReceived(self, data):
    message = loads(data)
    if message == self.factory.shutdown_params.message:
      try:
        self.transport.loseConnection()
      except:
        import sys
        print "MessageReceiver.disconnect failed: {0}".format(sys.exc_info()[1])
      self.factory.shutdown_params.deferred.callback(message)
    else:
      self.consumer.write(LumberjackMessage(self.sequence.next(), message))

#
# FIXME: This is set up as a factory/protocol, but it's really only designed to create a single
# protocol object, since there's only one consumer assigned. Maybe it could take a ConsumerFactory
# (via a callback perhaps, or a new interface?), but really the app is designed to manage a single
# socket connection from the parent and attach one reader to that. 
#
class MessageReceiverFactory(ClientFactory):
  protocol = MessageReceiver

  def __init__(self, consumer, shutdown_params):
    self.consumer = consumer
    self.shutdown_params = shutdown_params
    pass

  def buildProtocol(self, addr):
    p = ClientFactory.buildProtocol(self, addr)
    p.addConsumer(self.consumer)
    return p
