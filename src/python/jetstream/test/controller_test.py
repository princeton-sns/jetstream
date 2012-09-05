import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from controller import *
from worker import *
from computation_state import *
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
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 1)
    self.assertEquals(workerList[0].endpoint, self.client.sock.getsockname())


  def test_worker_liveness(self):
    # Use a smaller heartbeat interval to speed up this test
    hbInterval = 0.5
    self.controller.hbInterval = hbInterval
    worker1 = create_worker(self.controller.address, hbInterval)
    worker1.start_heartbeat_thread()
    worker2 = create_worker(self.controller.address, hbInterval)
    worker2.start_heartbeat_thread()
    time.sleep(hbInterval)

    # Initially the controller should see two alive workers
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 2)
    self.assertEquals(workerList[0].state, CWorker.ALIVE)
    self.assertEquals(workerList[1].state, CWorker.ALIVE)

    # Kill one of the workers, it should be marked dead after several hb intervals
    worker1.stop()
    time.sleep(hbInterval * (CWorker.DEFAULT_HB_DEAD_INTERVALS + 1))
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 1)
    self.assertEquals(workerList[0].state, CWorker.ALIVE)

    # Kill the second worker, this should terminate the liveness thread; starting
    # a new worker should create a new liveness thread
    worker2.stop()
    time.sleep(hbInterval * (CWorker.DEFAULT_HB_DEAD_INTERVALS + 1))
    self.assertEquals(len(self.controller.get_nodes()), 0)
    oldThread = self.controller.livenessThread
    worker3 = create_worker(self.controller.address, hbInterval)
    worker3.start_heartbeat_thread()
    time.sleep(hbInterval)
    self.assertEquals(len(self.controller.get_nodes()), 1)
    self.assertNotEqual(self.controller.livenessThread, oldThread)
    worker3.stop()
    
      
  def test_deploy(self):
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    worker1 = create_worker(self.controller.address)
    worker1.start_heartbeat_thread()
    worker2 = create_worker(self.controller.address)
    worker2.start_heartbeat_thread()
    time.sleep(2)
    # Deploy a single-operator topology
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    newOp = req.alter.toStart.add()
    newOp.op_typename = "cat /etc/shells"
    newOp.id.computationID = 1
    newOp.id.task = 1
    # Bind this operator to the second worker
    workerEndpoint = worker2.connection_to_server.getsockname()
    newOp.site.address = workerEndpoint[0]
    newOp.site.portno = workerEndpoint[1]
    
    buf = self.client.do_rpc(req, True)
    req = ControlMessage()
    req.ParseFromString(buf)
    self.assertEquals(req.type, ControlMessage.OK)
    # Wait for the topology to start running; there should be one task on the
    # second worker and none on the first
    time.sleep(1)
    self.assertEquals(len(worker2.tasks), 1)
    self.assertEquals(len(worker1.tasks), 0)
    print "stopping worker"
    worker1.stop()
    worker2.stop()

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
