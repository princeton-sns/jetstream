import asyncore
import asynchat

import logging
import socket
import struct
import subprocess
import threading
import time


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  bind_port = DEFAULT_BIND_PORT
  
  address = ('localhost', bind_port) 
  server = JetStreamServer(address)
  asyncore.loop()



class JetStreamServer(asyncore.dispatcher):
  """Receives connections and establishes handlers for each client.
  Based on code from http://blog.doughellmann.com/2009/03/pymotw-asynchat.html
  """
  
  def __init__(self, address):
    asyncore.dispatcher.__init__(self)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.bind(address)
    self.address = self.socket.getsockname()
    print "server bound to",address
    self.listen(1)
    return

  def handle_accept(self):
    # Called when a client connects to our socket
    client_info = self.accept()
    ConnHandler(sock=client_info[0])
    
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
    self.close()      


class ConnHandler(asynchat.async_chat):
  """Handles echoing messages from a single client.
  """
  
  def __init__(self, sock):
    self.received_data = []
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
    print "buffer read"
#        self.process_data()
    print ''.join(self.received_data)

if __name__ == '__main__':
  main()
  sys.exit(0)