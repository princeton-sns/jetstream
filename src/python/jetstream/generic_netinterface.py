import asyncore
import asynchat

import logging
import socket
import struct
import threading
import time

from jetstream_types_pb2 import *

logger = logging.getLogger('JetStream')

class JSServer(asyncore.dispatcher):
  """Receives connections and establishes handlers for each client.
  Based on code from http://blog.doughellmann.com/2009/03/pymotw-asynchat.html
  """
  
  def __init__(self, address):
    asyncore.dispatcher.__init__(self)
    
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.bind(address)
    self.address = self.socket.getsockname()
    print "server bound to",address
    self.stopped = False
    self.listen(1)
    return

  def handle_accept(self):
    client_info = self.accept()
    ConnHandler(sock=client_info[0], server=self)
    return
  
  def handle_close(self):
    self.close()
    
  def stop(self):
    self.stopped = True
    self.close()      

  def start_as_thread(self):
    t = threading.Thread(group = None, target =self.evtloop, args = ())
    t.daemon = True
    t.start()

  def evtloop(self):
    try:
      asyncore.loop()
    except Exception as e:
      if not self.stopped:
        print e
    
    
  def process_message(self, buf, handler):
       raise "Subclasses must override this"
       

class ConnHandler(asynchat.async_chat):
  """Handles incoming messages from a single client.
  """
  
  def __init__(self, sock, server):
    self.received_data = []
    self.server = server
#        self.logger = logging.getLogger('EchoHandler%s' % str(sock.getsockname()))
    asynchat.async_chat.__init__(self, sock)
    pbframe_len = sock.recv(4)
    print "Got %d bytes" % len(pbframe_len)
    unpacked_len = struct.unpack("!l", pbframe_len)[0]
    print "expect to get %d bytes next"  % ( unpacked_len )
    self.set_terminator(unpacked_len)
    return

  def collect_incoming_data(self, data):
    logger.debug('collect_incoming_data() -> (%d)\n"""%s"""', len(data), data)
    self.received_data.append(data)

  def found_terminator(self):
    """The end of a message has been seen."""
    logger.debug('found_terminator()')
    buf =  ''.join(self.received_data)
    self.server.process_message(buf, self)

  def send_pb(self, response):  #name 'send' already in use for socket send
    buf = response.SerializeToString()
    self.push( struct.pack("!l", len(buf)))
    self.push(buf)

