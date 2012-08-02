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
    self.server = Controller(('localhost', 0))
    self.server.start_as_thread()
    print "connecting to %s:%d" % self.server.address
    self.client = JSClient(self.server.address)

  def tearDown(self):
    print "Closing client in net test"
    self.client.close()
    print "Stopping server in net test"
    self.server.stop()
    
  def test_connect(self):
    # Test the connection by a simple GET_NODES call
    req = ServerRequest()
    req.type = ServerRequest.GET_NODES

    buf = self.client.do_rpc(req, True)
    resp = ServerResponse()
    resp.ParseFromString(buf)
    
    self.assertEquals(resp.count_nodes, 0)

  def test_heartbeat(self):
    req = ServerRequest()
    req.type = ServerRequest.HEARTBEAT
    req.heartbeat.freemem_mb = 3900
    req.heartbeat.cpuload_pct = 90
    buf = self.client.do_rpc(req, False)
    # Since no response is expected, sleep a little to give the server time to process message
    time.sleep(1)

  def test_deploy(self):
    req = ServerRequest()
    req.type = ServerRequest.DEPLOY
    newTask = TaskMeta()
    newTask.cmd = "cat /etc/shells"
    newTask.id.computationID = 1
    newTask.id.task = 1
    #FIXME: Why does append() not work??
    req.alter.toStart.extend([newTask])
    buf = self.client.do_rpc(req, True)

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
