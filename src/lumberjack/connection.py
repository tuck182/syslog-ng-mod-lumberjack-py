from lumberjack.lumberjack_socket import * 
from lumberjack.util import *

import collections
import random
import socket
import time

class Connection(object):
  _servers = []
  _socket = None
  _queue = None
  
  def __init__(self, queue, servers, **kwargs):
    self._kwargs = kwargs
    if servers is None or isinstance(servers, collections.Sequence) and not servers:
      raise Exception("Must set at least one hostname")
    if isinstance(servers, basestring):
      servers = [servers]
      
    self._servers = servers
    self._queue = queue
    
    self.start()

  def start(self):
    self._connect()

  def send(self, message, message_id):
    if self._socket is None:
      try:
        self._connect()
      except:
        self._queue.nack_all()
        return
    try:
      self._socket.write(message, message_id)
    except SocketException:
      # This will nack_all, so we don't need to do it explicitly
      self._socket.close(graceful = False)
      return
      
  def shutdown(self, graceful = False):
    self._socket.close(graceful)

  def socket_closed(self):
    self._queue.nack_all()
    self._socket = None

  def acknowledge_sent(self, message_id):
    self._queue.acknowledge(message_id)
  
  def _connect(self):
    servers = list(self._servers)
    random.shuffle(servers)

    while servers:
      try:
        (hostname, port) = self._split_server(servers.pop())
        self._socket = LumberjackSocket(hostname = hostname,
                              port = port,
                              listeners = [self],
                              **self._kwargs)
        return
      except socket.error as e:
        print("Connection failed to {0}:{1}: {2}".format(hostname, port, e))

      time.sleep(1)
      raise Exception("Could not connect to any hosts")
    
  def _reconnect(self):
    try:
      self.disconnect()
    except socket.error:
      pass
    self._connect()

  def _split_server(self, server):
    try:
      (hostname, port) = server.split(':')
    except ValueError:
      (hostname, port) = (server, DEFAULT_PORT)
    return (hostname, int(port)) 
