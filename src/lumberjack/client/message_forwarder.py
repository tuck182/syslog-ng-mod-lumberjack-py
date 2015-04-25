from zope.interface import implementer, implements

from twisted.internet import interfaces, defer
from twisted.internet.abstract import _ConsumerMixin

from collections import deque

@implementer(interfaces.IConsumer, interfaces.ILoggingContext)
class RetryingMessageForwarder(_ConsumerMixin):
  """
  """
  implements(interfaces.IPushProducer, interfaces.IConsumer)

  connected = 0
  disconnected = 0
  disconnecting = 0
  
  paused = True
  
  # FIXME: Need to limit size of message_queue for cases where connection can't be made
  message_queue = deque()
  consumers = []
  
  def addConsumer(self, consumer):
    consumer.registerProducer(self, True)
    self.consumers.append(consumer)
    self.connected = 1
    self.empty_queue()
  
  def removeConsumer(self, consumer):
    consumer.unregisterProducer()
    self.consumers.remove(consumer)
    self.connected = len(self.consumers) > 0
  
  # IConsumer
  def write(self, message):
    message.handler = defer.Deferred()
    message.handler.addCallback(self.ack_message)
    message.handler.addErrback(self.nak_message)
    
    if self.consumers and not self.paused:
      self.send_message(message)
    else:
      self.message_queue.appendleft(message)

  def send_message(self, message):
    for c in self.consumers:
      c.write(message)
      
  def ack_message(self, message):
    pass

  def nak_message(self, failure):
    message = failure.value
    self.message_queue.append(message)

  def empty_queue(self):
    while self.consumers and not self.paused and self.message_queue:
      self.send_message(self.message_queue.pop())

  # IPushProducer
  def stopProducing(self):
    # print("RetryingMessageForwarder.stopProducing")
    pass
  
  def pauseProducing(self):
    # print("RetryingMessageForwarder.pauseProducing")
    self.paused = True
    pass
  
  def resumeProducing(self):
    # print("RetryingMessageForwarder.resumeProducing")
    self.paused = False
    self.empty_queue()
    pass
