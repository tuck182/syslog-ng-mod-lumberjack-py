from lumberjack import *
from lumberjack.encoder import Encoder
from lumberjack.util import *

import socket
import ssl
import struct
import traceback
import zlib
from collections import deque

__all__ = [
  'LumberjackSocket', 
  'SocketException', 
  'SocketClosed',
  'UnknownPacket',
  'RecvTimeout'
]

class SocketException(Exception):
    pass

class SocketClosed(SocketException):
    pass

class UnknownPacket(SocketException):
    pass

class RecvTimeout(SocketException):
    pass

class LumberjackSocket(object):
  _ssl_socket = None
  _unacked_ids = None
  _sequence = 0
  _last_ack = 0
  _pending_bytes = ""
  _timeout = None
  
  def __init__(self, 
               hostname,
               listeners = [], 
               port = DEFAULT_PORT, 
               ssl_certificate = None, 
               timeout = 0,
               **kwargs):
      
    if hostname is None:
      raise Exception("Must set hostname")
    if port is None or port == 0:
      raise Exception("Invalid port: {0}".format(port))
    if ssl_certificate is None:
      raise Exception("Must set ssl certificate or path")
    
    self._listeners = listeners
    self._timeout = timeout
    self._unacked_ids = deque()
    self._connect(hostname, port, ssl_certificate, timeout)
    
  def write(self, message, message_id = None):
    if isinstance(message, dict):
      return self._write_dict(message, message_id)
    compressed = zlib.compress(message)
    size = strlen(compressed)
    payload = Encoder.pack("!ccI{0}s".format(size), '1', 'C', size, compressed)
    result = self._send(payload)

  def close(self, graceful = False):
    if graceful:
      try:
        self._ack_all(block = True)
      except SocketException:
        pass
    try:
      self._ssl_socket.shutdown(socket.SHUT_RDWR)
    except socket.error:
      pass
    try:
      self._ssl_socket.close()
    except socket.error:
      pass
    self._ssl_socket = None
    self._notify_listeners(lambda l: l.socket_closed())
    
  def _notify_listeners(self, f):
    for listener in self._listeners:
      try:
        f(listener)
      except:
        traceback.print_exc()

  def _connect(self, hostname, port, ssl_certificate, timeout):
    tcp_socket = socket.create_connection((hostname, port), timeout)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    ssl_context.load_verify_locations(cafile = ssl_certificate)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    self._ssl_socket = ssl_context.wrap_socket(tcp_socket)
    payload = Encoder.pack("!ccI", '1', 'W', 0) # window_size
    self._send(payload)
    
  def _send(self, payload):
    if self._ssl_socket.gettimeout() != self._timeout:
      self._ssl_socket.settimeout(self._timeout)
    total_sent = 0
    msg_len = len(payload)
    while total_sent < msg_len:
      sent = self._ssl_socket.send(payload[total_sent:])
      if sent == 0:
          raise Exception("socket connection broken")
      total_sent += sent
    return total_sent

  def _write_dict(self, message, message_id):
    self._unacked_ids.append(message_id)
    frame = Encoder.to_compressed_frame(message, message_id)
    self.write(frame)
    self._ack_all(block = True)

  def _ack_all(self, block = False):
    while self._unacked_ids:
      message_id = self._ack_one(block)
      if not block and message_id is None:
        return
      self._unacked_ids.popleft()
      self._notify_listeners(lambda l: l.acknowledge_sent(message_id))
  
  def _ack_one(self, block = False):
    try:
      (version, type, message_id) = struct.unpack("!ccI", self._read_packet(6, block))
      if type != 'A':
        raise UnknownPacket("Whoa we shouldn't get this frame: {0}".format(type))
      return message_id
    except RecvTimeout:
      if not block:
        raise Exception("Receive timed out on a blocking read; this should never happen")
      return None

  def _read_packet(self, size, block = False):
    if block and self._ssl_socket.gettimeout() != self._timeout:
      self._ssl_socket.settimeout(self._timeout)
    elif not block and self._ssl_socket.gettimeout() != 0:
      self._ssl_socket.settblocking(0)
    try:
      bytes = self._pending_bytes
      while strlen(bytes) < size:
        result = self._ssl_socket.recv(size - strlen(bytes))
        if not result:
          raise SocketClosed("Got 0 bytes; socket is closed?")
        bytes += result
        if not block and strlen(bytes) < size:
          self._pending_bytes = bytes
          raise RecvTimeout()
      self._pending_bytes = ""
      return bytes
    except socket.error as e:
      if block:
        traceback.print_exc()
        raise Exception("Got a socket error when attempting a blocking read")
      print "Got a socket error from a non-blocking read; assuming everything is fine"
      raise RecvTimeout()
