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

class TestController(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))
    self.controller.start_as_thread()
    print "controller bound to %s:%d" % self.controller.address
    self.client = JSClient(self.controller.address)

  def tearDown(self):
    self.client.close()
    self.controller.stop()
    
  def test_connect(self):
    # Test the connection by a simple GET_NODES call
    req = ControlMessage()
    req.type = ControlMessage.GET_NODE_LIST_REQ

    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    
    self.assertEquals(resp.node_count, 0)

  def test_heartbeat(self):
    req = ControlMessage()
    req.type = ControlMessage.HEARTBEAT
    req.heartbeat.freemem_mb = 3900
    req.heartbeat.cpuload_pct = 90
    buf = self.client.do_rpc(req, False)
    # Since no response is expected, sleep a little to give the controller time to process message
    time.sleep(1)
    self.assertEquals(len(self.controller.get_nodes()), 1)

  def test_deploy(self):
    
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    worker = create_worker(self.controller.address)
    worker.start_heartbeat_thread()
    time.sleep(2)
    # Tell the controller to deploy a topology (it will then deploy it on the worker)
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    newTask = TaskMeta()
    newTask.op_typename = "cat /etc/shells"
    newTask.id.computationID = 1
    newTask.id.task = 1
    # Get a worker address in the right format (note getaddrinfo returns a list of addresses)
    workerAddr = socket.getaddrinfo('localhost', DEFAULT_WORKER_BIND_PORT)[0][4]
    newTask.site.address = workerAddr[0]
    newTask.site.portno = workerAddr[1]
    #FIXME: Why does append() not work??
    req.alter.toStart.extend([newTask])
    
    buf = self.client.do_rpc(req, True)
    print "deploy finished..."
    # Wait for the topology to start running on the worker
    time.sleep(2)
    self.assertEquals(len(worker.tasks), 1)
    print "stopping worker"
    worker.stop()

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
