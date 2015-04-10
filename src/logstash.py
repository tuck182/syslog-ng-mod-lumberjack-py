#!/usr/bin/env python2.7

LOGSTASH_CONF = "/etc/logstash-forwarder.conf"

# Sample message from syslog:
# {'HOST_FROM': u'eione', 
#  'FACILITY': u'syslog', 
#  'SEQNUM': u'1', 
#  'TAGS': u'.source.s_src', 
#  'PID': u'29383', 
#  'PRIORITY': u'notice', 
#  'SOURCE': u's_src', 
#  'HOST': u'eione', 
#  'PROGRAM': u'syslog-ng', 
#  'DATE': u'Apr  7 18:32:49', 
#  'MESSAGE': u"syslog-ng starting up; version='3.5.6'", 
#  'SOURCEIP': u'127.0.0.1'}

import hjson
import sys
import traceback
import lumberjack

from datetime import datetime
from lumberjack import Client

DEFAULT_CONFIG = {
                  'network': {
                              'servers': None,
                              'ssl-ca': None,
                              'timeout': lumberjack.DEFAULT_TIMEOUT,
                              'window-size': lumberjack.DEFAULT_WINDOW_SIZE
                              }
                  }


logstash_client = None

def init():
  global logstash_client

  try:  
    with open(LOGSTASH_CONF) as data_file:    
      config = _merge(dict(DEFAULT_CONFIG), hjson.load(data_file))
  
    logstash_client = Client(servers = config['network']['servers'], 
                             ssl_certificate = config['network']['ssl-ca'],
                             window_size = config['network']['window-size'],
                             timeout = config['network']['timeout'])
    return True
  except:
    traceback.print_exc()
    print("Error was a {0}".format(_fullname(sys.exc_info()[1])))
    raise

def deinit(graceful = False):
  global logstash_client
  
  if not logstash_client is None:
    logstash_client.shutdown(graceful)
    logstash_client = None
  return True

def queue(message):
  global logstash_client

  try:
    logstash_client.write(_convert_message(message))
  except BaseException as e:
    print("failed")
    traceback.print_exc()
    print("Error was a {0}".format(_fullname(e)))
  except:
    print("failed unexpectedly")
    traceback.print_exc()
    print("Error was a {0}".format(_fullname(sys.exc_info()[1])))
  finally:
    return True

def _merge(a, b, path = None):
  "merges b into a"
  if path is None: path = []
  for key in b:
    if key in a:
      if isinstance(a[key], dict) and isinstance(b[key], dict):
        _merge(a[key], b[key], path + [str(key)])
      else:
        a[key] = b[key]
    else:
      a[key] = b[key]
  return a

def _convert_message(message):
  defaults = {}
  for k in ['MESSAGE', 'HOST_FROM', 'PROGRAM', 'SEQNUM', 'FACILITY', 'PRIORITY', 'DATE',
            'HOST', 'SOURCEIP', 'SOURCE', 'PID', 'TAGS']:
    defaults[k] = None
  defaults.update(message)
  message = defaults
  result = _remove_none_values({
          'line':     message['MESSAGE'],
          'host':     message['HOST_FROM'],
          'file':     message['PROGRAM'],
          'offset':   message['SEQNUM'],
          'facility': message['FACILITY'],
          'priorty':  message['PRIORITY'],
          'date':     message['DATE'] or _date_now(),
          'syslog-ng': {
            'host':       message['HOST'],
            'source-ip':  message['SOURCEIP'],
            'source':     message['SOURCE'],
            'pid':        message['PID'],
            'tags':       message['TAGS']
          }
  }, frozenset(['line', 'host', 'file', 'offset']))
  return result

def _remove_none_values(dict, preserve_keys = frozenset()):
  if hasattr(dict, "iteritems") and callable(getattr(dict, "iteritems")):
    return {k: _remove_none_values(v, preserve_keys) for k, v in dict.iteritems() 
            if v is not None or k in preserve_keys}
  return dict

def _date_now():
  now = datetime.now()
  '%s %2d %s' % (now.strftime('%b'), now.day, now.strftime('%H:%M:%S'))
  
def _fullname(o):
  try:
    return o.__module__ + "." + o.__class__.__name__
  except:
    return "unknown object: {0}".format(o)
  
def main(args):
  import socket
  hostname = socket.gethostbyaddr(socket.gethostname())[0]
  init()
  for line in args:
    message = {'HOST_FROM':  hostname,
               'HOST':       hostname,
               'FACILITY':   'user',
               'PRIORTY':    'notice', 
               'PROGRAM':    'logstash.py',
               'DATE':       _date_now(),
               'MESSAGE':    line
    }
    queue(message)
  deinit(graceful = True)

if __name__ == "__main__":
  import sys
  main(sys.argv[1:])
