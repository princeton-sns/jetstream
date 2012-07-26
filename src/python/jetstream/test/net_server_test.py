import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from jetstream.server_netinterface import *
from jetstream.operator_graph import OperatorGraph,Operators
from jetstream.gen.jetstream_types_pb2 import *


class TestRemoteServer(unittest.TestCase):

  def setUp(self):
    pass
#    self.server = LocalServer()
    
  def test_connect(self):
    server = JetStreamServer( ('localhost', 0) )
    print "connecting to",server.address
    sock = socket.create_connection(server.address, 1)
    sock.send(  struct.pack("!l", 10))
    sock.send( "1234567890abcd")

    print "sent"
    
    
    t = threading.Thread(group = None, target =asyncore.loop, args = ()).start()
    print "continuing..."
    time.sleep(1)
    server.stop()

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)