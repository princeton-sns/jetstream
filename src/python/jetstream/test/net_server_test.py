import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from server_netinterface import *
from generic_netinterface import JSClient

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
    
    print "connecting to %s:%d" % server.address
    client = JSClient(server.address)
    
    req = ServerRequest()
    req.type = ServerRequest.GET_NODES

    buf = client.do_rpc(req)
    resp = ServerResponse()
    resp.ParseFromString(buf)
    
    self.assertEquals(resp.count_nodes, 0)
    server.stop()

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
