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
    self.my_sockets = {}

    asyncore.dispatcher.__init__(self, map=self.my_sockets)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    logger.info("server binding to %s:%d" % (address[0], address[1]) )
    self.bind(address)
    self.addr_to_handler = {}
    self.address = self.socket.getsockname()
    logger.info("server bound to %s:%d" % (self.address[0], self.address[1]) )
    self.stopped = False
    self.thread = None
    self.listen(1)
    return

  def handle_accept(self):
    client_info = self.accept()
    while client_info is None:
      logger.warn("accept failed, retrying")
      client_info = self.accept()
    logger.debug("accepted connection from %s:%d" % client_info[1])
 #     logger.warn("Didn't expect None return from accept in handler running on %s -- what's broken?" % str(self.address))
#      return
    h = ConnHandler(sock=client_info[0], server=self, cli_addr=client_info[1], map=self.my_sockets)
    self.addr_to_handler[client_info[1]] = h
    return
  
  def connect_to(self, dest_addr):
    # If a handler for the destination exists, then it is still valid as of this check (otherwise it
    # would have been removed by ConnHandler.handle_close()). 
    if dest_addr in self.addr_to_handler:
      return self.addr_to_handler[dest_addr]
    s = socket.create_connection(dest_addr)
    s.setblocking(0)
    h = ConnHandler(sock=s, server=self, cli_addr=dest_addr, map=self.my_sockets)
#    print "client connected to %s:%d" % dest_addr
    self.addr_to_handler[dest_addr] = h
    return h
  
  def handle_close(self):
    self.close()
    
  def stop(self):
    self.stopped = True
    self.close()
    # Close any outgoing connections we have initiated (since these are part of our socket map,
    # they will prevent asyncore.loop() from exiting)
    for h in self.addr_to_handler.values():
      h.close()
    if (self.thread != None) and (self.thread.is_alive()):
      self.thread.join()

  def start_as_thread(self):
    self.thread = threading.Thread(group=None, target=self.evtloop, args=())
    self.thread.daemon = True
    self.thread.start()

  def evtloop(self):
    try:
      # Use a low timeout so the loop terminates when all channels have been closed. For some reason
      # the default is 30 seconds, which takes forever.
      asyncore.loop(map=self.my_sockets, timeout=1)
    except Exception as e:
      if not self.stopped:
        print "Exception caught leaving loop",e
    
     
  def process_message(self, buf, handler):
    raise "Subclasses must override this"
       

class ConnHandler(asynchat.async_chat):
  """Handles incoming messages from a single client.
  Specializes async_chat for the case where communication is by length-prefixed
  ProtoBufs records
  """
    
  def __init__(self, sock, server, cli_addr, map):
    self.received_data = []
    self.server = server
    self.cli_addr = cli_addr
    self.pushLock = threading.Lock()
    self.next_frame_len = -1
    self.set_terminator(4)
#        self.logger = logging.getLogger('EchoHandler%s' % str(sock.getsockname()))
    asynchat.async_chat.__init__(self, sock, map)
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
  
  # Overwrite to make thread-safe
  def push(self, data):
    with self.pushLock:
      asynchat.async_chat.push(self, data)

  def send_pb(self, response):  #name 'send' already in use for socket send
    buf = response.SerializeToString()
    # Use one call to push to guarantee protobuf is sent contiguously
    self.push(struct.pack("!l", len(buf)) + buf)

  def handle_close(self):
    logger.info("Socket closed by remote end %s:%d" % self.cli_addr)
    self.close()
    if self.cli_addr in self.server.addr_to_handler:
      del self.server.addr_to_handler[self.cli_addr]



class JSClient():
  """Simple synchronous client that speaks appropriate protobuf interface"""
  def __init__(self, address):
    self.sock = socket.create_connection(address, 1)

  def do_rpc(self, req, expectResponse):
    buf = req.SerializeToString()
    
    self.sock.send(  struct.pack("!l", len(buf)))
    self.sock.send(buf)
    if expectResponse:
      pbframe_len = self.sock.recv(4)
      unpacked_len = struct.unpack("!l", pbframe_len)[0]
      print "JSClient sent req, got back response of length %d" % unpacked_len
      # print "reading another %d bytes" % unpacked_len
      buf = self.sock.recv(unpacked_len)
      return buf
    
  def close(self):
    self.sock.close()
