import asyncore
import asynchat

import logging
import socket
import struct
import threading
import time

from jetstream_types_pb2 import *

logger = logging.getLogger('JetStream')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class JSServer(asyncore.dispatcher):
  """Generic network-server connection handler for JetStream.
  Receives connections and establishes handlers for each client.
  Delegates handling to subclass process_message method
  Based on code from http://blog.doughellmann.com/2009/03/pymotw-asynchat.html
  """
  
  def __init__(self, address):
    asyncore.dispatcher.__init__(self)
    
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    self.bind(address)
    self.addr_to_handler = {}
    self.address = self.socket.getsockname()
    logger.info("server bound to %s:%d" % self.address)
    self.stopped = False
    self.listen(1)
    return

  def handle_accept(self):
    client_info = self.accept()
    
    h = ConnHandler(sock=client_info[0], server=self, cli_addr = client_info[1])
    self.addr_to_handler[client_info[1]] = h
    return
  
  def connect_to(self, dest_addr):
    
    s = socket.create_connection(dest_addr)
    h = ConnHandler(sock=s, server=self, cli_addr = dest_addr)
    self.addr_to_handler[dest_addr] = h
    return h
  
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
  Specializes async_chat for the case where communication is by length-prefixed
  ProtoBufs records
  """
  
  
  def __init__(self, sock, server, cli_addr):
    self.received_data = []
    self.server = server
    self.cli_addr = cli_addr
    self.next_frame_len = -1
    self.set_terminator(4)
#        self.logger = logging.getLogger('EchoHandler%s' % str(sock.getsockname()))
    asynchat.async_chat.__init__(self, sock)

    return

  def collect_incoming_data(self, data):
    logger.debug('collect_incoming_data() -> (%d)', len(data))

    if self.next_frame_len == -1:
      pbframe_len = data[0:4]
      unpacked_len = struct.unpack("!l", pbframe_len)[0]
      self.received_data.append(data[4:])
      self.next_frame_len = unpacked_len #can't reset terminator here; async_chat will clobber it
    else:  
      self.received_data.append(data)
    

  def found_terminator(self):
    """The end of a message has been seen."""
    logger.debug('found_terminator()')
    buf =  ''.join(self.received_data)
    self.received_data = []
    if self.next_frame_len > 0: #should only happen right after reading length
      assert len(buf) == 0  
      self.set_terminator(self.next_frame_len)
      self.next_frame_len = 0 
    else:
      if len(buf) == 0:
        return
      self.server.process_message(buf, self)
      self.set_terminator(4)
      self.next_frame_len = -1
    

  def send_pb(self, response):  #name 'send' already in use for socket send
    buf = response.SerializeToString()
    self.push( struct.pack("!l", len(buf)))
    self.push(buf)

  def handle_close(self):
    logger.info("Socket closed by remote end %s:%d" % self.cli_addr)
    self.close()
    if self.cli_addr in self.server.addr_to_handler:
      del self.server.addr_to_handler[self.cli_addr]
