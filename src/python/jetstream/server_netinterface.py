import asyncore
import asynchat

import logging
import socket
import struct
import subprocess
import threading
import time

from jetstream.gen.jetstream_types_pb2 import *
from jetstream.gen.jetstream_controlplane_pb2 import *
from jetstream.server import Server


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  bind_port = DEFAULT_BIND_PORT
  
  address = ('localhost', bind_port) 
  server = JetStreamServer(address)
  asyncore.loop()


class Coordinator(Server):
  
  def get_nodes(self):
    return []

class JetStreamServer(asyncore.dispatcher):
  """Receives connections and establishes handlers for each client.
  Based on code from http://blog.doughellmann.com/2009/03/pymotw-asynchat.html
  """
  
  def __init__(self, address):
    asyncore.dispatcher.__init__(self)
    self.coordinator = Coordinator()

    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.bind(address)
    self.address = self.socket.getsockname()
    print "server bound to",address
    self.stopped = False
    self.listen(1)
    return

  def handle_accept(self):
    # Called when a client connects to our socket
    client_info = self.accept()
    ConnHandler(sock=client_info[0], coordinator= self.coordinator)
    
    # We only want to deal with one client at a time,
    # so close as soon as we set up the handler.
    # Normally you would not do this and the server
    # would run forever or until it received instructions
    # to stop.
#        self.handle_close()
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

class ConnHandler(asynchat.async_chat):
  """Handles echoing messages from a single client.
  """
  
  def __init__(self, sock, coordinator):
    self.received_data = []
    self.coordinator = coordinator
#        self.logger = logging.getLogger('EchoHandler%s' % str(sock.getsockname()))
    asynchat.async_chat.__init__(self, sock)
    pbframe_len = sock.recv(4)
    print "Got %d bytes" % len(pbframe_len)
    unpacked_len = struct.unpack("!l", pbframe_len)[0]
    print "expect to get %d bytes next"  % ( unpacked_len )
    # Start looking for the ECHO command
#        self.process_data = self._process_command
    self.set_terminator(unpacked_len)
    return

  def collect_incoming_data(self, data):
    """Read an incoming message from the client and put it into our outgoing queue."""
    logger.debug('collect_incoming_data() -> (%d)\n"""%s"""', len(data), data)
    self.received_data.append(data)

  def found_terminator(self):
    """The end of a command or message has been seen."""
    logger.debug('found_terminator()')
    print "buffer read; parsing"

    buf =  ''.join(self.received_data)
    req = ServerRequest()
    req.ParseFromString(buf)
    print req
    response = ServerResponse()
    response.count_nodes = len(self.coordinator.get_nodes())
    if req.type == ServerRequest.GET_NODES:
      node_list = self.coordinator.get_nodes()
      response.nodes.extend(node_list)
    #elif req.type == ServerRequest.DEPLOY:
    #  self.coordinator.deploy(req.alter)
    buf = response.SerializeToString()
    self.push( struct.pack("!l", len(buf)))
    self.push(buf)

if __name__ == '__main__':
  main()
  sys.exit(0)