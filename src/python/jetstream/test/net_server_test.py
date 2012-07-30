import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from server_netinterface import *
from operator_graph import OperatorGraph,Operators
from jetstream_types_pb2 import *
from jetstream_controlplane_pb2 import *


class TestRemoteServer(unittest.TestCase):

  def setUp(self):
    pass
#    self.server = LocalServer()
    
  def test_connect(self):
    server = get_server_on_this_node()
    server.start_as_thread()
    
    print "connecting to",server.address
    sock = socket.create_connection(server.address, 1)
    
    req = ServerRequest()
    req.type = ServerRequest.GET_NODES
    buf = req.SerializeToString()
    
    sock.send(  struct.pack("!l", len(buf)))
    sock.send(buf)
    print "sent"
    time.sleep(1)
    pbframe_len = sock.recv(4)
    print "got back response of length %d" % len(pbframe_len)
    unpacked_len = struct.unpack("!l", pbframe_len)[0]
    print "reading another %d bytes" % unpacked_len
    buf = sock.recv(unpacked_len)
    resp = ServerResponse()
    resp.ParseFromString(buf)
    print resp
    server.stop()

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
