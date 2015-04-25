import os

from twisted.internet import abstract, defer, error, fdesc

class AdoptFileDescriptor(abstract.FileDescriptor):
  def __init__(self, fd, reactor = None):
    super(AdoptFileDescriptor, self).__init__(reactor)
    self.fd = fd

  def fileno(self):
    return self.fd

  def closeDesciptor(self):
    os.close(self.fd)

  def connectionLost(self,reason):
    super(AdoptFileDescriptor, self).connectionLost(reason)
    self.closeDesciptor()


class AdoptReadDescriptor(AdoptFileDescriptor):
  def __init__(self, factory, fd, reactor = None):
    # Read from standard input, make it Non Blocking
    self._fd = fd
    self.factory = factory
    fdesc.setNonBlocking(fd)
    # We open a file to read from 
    super(AdoptReadDescriptor, self).__init__(fd, reactor)
     
  def startReading(self):
    self.factory.doStart()
    self.protocol = self.factory.buildProtocol(None)
    self.protocol.makeConnection(self)
    super(AdoptReadDescriptor, self).startReading()

  def doRead(self):
    fdesc.readFromFD(self._fd, self.protocol.dataReceived)


class FileDescriptorEndpoint:
    """
    An endpoint for listening on a file descriptor initialized outside of
    Twisted.

    @ivar _used: A C{bool} indicating whether this endpoint has been used to
        listen with a factory yet.  C{True} if so.
    """
    _close = os.close
    _setNonBlocking = staticmethod(fdesc.setNonBlocking)

    def __init__(self, reactor, fileno):
        """
        @param reactor: An L{IReactorSocket} provider.

        @param fileno: An integer file descriptor corresponding to a listening
            I{SOCK_STREAM} socket.
        """
        self.reactor = reactor
        self.fileno = fileno
        self._used = False

    def listen(self, factory):
        """
        Implement L{IStreamServerEndpoint.listen} to start listening on, and
        then close, C{self._fileno}.
        """
        if self._used:
            return defer.fail(error.AlreadyListened())
        self._used = True

        try:
            self._setNonBlocking(self.fileno)
            
            reader = AdoptReadDescriptor(factory, self.fileno, self.reactor)
            reader.startReading()
        except:
            return defer.fail()
        return defer.succeed(reader)
