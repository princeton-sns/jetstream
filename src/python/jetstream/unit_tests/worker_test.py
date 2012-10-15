import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from controller import *
from worker import *
from generic_netinterface import JSClient

from jetstream_types_pb2 import *


class TestWorker(unittest.TestCase):

  def setUp(self):
    pass
#    self.server = LocalServer()
    
  def test_connect(self):
    server = get_server_on_this_node()
    server.start()
    
    time.sleep(1)
    worker = create_worker(server.address)
    worker.start()
    
#    while True:
#      print "main thread is pausing..."
    time.sleep(5)
    
    print "connecting to %s:%d" % server.address
    
    client = JSClient(server.address)
    req = ControlMessage()
    req.type = ControlMessage.GET_NODE_LIST_REQ

    buf = client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    
    self.assertEquals(resp.node_count, 1)
    print resp

    worker.stop()
    client.close()
    server.stop()
    print "Done"

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
