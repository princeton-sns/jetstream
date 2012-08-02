import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from server_netinterface import *
from worker import *
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
    
    time.sleep(1)
    worker = create_worker(server.address)
    
    time.sleep(1) #wait for connect to server to be ready before we start beating
    worker.start_heartbeat_thread()
    
#    while True:
#      print "main thread is pausing..."
    time.sleep(8)
    
    print "connecting to %s:%d" % server.address
    
    client = JSClient(server.address)
    req = ServerRequest()
    req.type = ServerRequest.GET_NODES

    buf = client.do_rpc(req)
    resp = ServerResponse()
    resp.ParseFromString(buf)
    
    self.assertEquals(resp.count_nodes, 1)
    print resp

    worker.stop()
    client.close()
    server.stop()
    print "Done"

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
