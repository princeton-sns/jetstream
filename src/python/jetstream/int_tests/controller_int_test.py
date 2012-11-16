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
from query_graph import Operator


from jetstream_types_pb2 import *

class TestController(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))
    self.controller.start()
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
    addr = self.client.sock.getsockname()
    req.heartbeat.dataplane_addr.address = addr[0]
    req.heartbeat.dataplane_addr.portno = addr[1]
    buf = self.client.do_rpc(req, False)
    # Since no response is expected, sleep a little to give the controller time to process message
    time.sleep(1)
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 1)
    self.assertEquals(workerList[0].endpoint, self.client.sock.getsockname())


  def test_worker_liveness(self):

    print "\n--- test worker liveness ---"
    # Use a smaller heartbeat interval to speed up this test
    hbInterval = 0.5
    self.controller.hbInterval = hbInterval
    worker1 = create_worker(self.controller.address, hbInterval)
    worker1.start()
    worker2 = create_worker(self.controller.address, hbInterval)
    worker2.start()
    time.sleep(hbInterval)

    # Initially the controller should see two alive workers
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 2)
    self.assertEquals(workerList[0].state, CWorker.ALIVE)
    self.assertEquals(workerList[1].state, CWorker.ALIVE)

    # Stop sending heartbeats from one of the workers; it should be marked dead after
    # several hb intervals
    worker1.stop_heartbeat_thread()
    time.sleep(hbInterval * (CWorker.DEFAULT_HB_DEAD_INTERVALS + 1))
    workerList = self.controller.get_nodes()
    self.assertEquals(len(workerList), 1)
    self.assertEquals(workerList[0].state, CWorker.ALIVE)

    # Kill the second worker; it should be marked dead much faster since we're closing the socket
    worker2.stop()
    time.sleep(1)
    self.assertEquals(len(self.controller.get_nodes()), 0)
    
      
  def test_deploy(self):
    print "--- test deploy ---"
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    worker1 = create_worker(self.controller.address)
    worker1.start()
    worker2 = create_worker(self.controller.address)
    worker2.start()
    time.sleep(2)
    # Deploy a single-operator topology
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    req.alter.computationID = 17
    newOp = req.alter.toStart.add()
    newOp.op_typename = Operator.OpType.UNIX
    cfg = newOp.config.add()
    cfg.opt_name = "cmd"
    cfg.val = "cat /etc/shells"
    newOp.id.computationID = req.alter.computationID
    newOp.id.task = 1
    # Bind this operator to the second worker
    workerEndpoint = worker2.controllerConn.getsockname()
    newOp.site.address = workerEndpoint[0]
    newOp.site.portno = workerEndpoint[1]
        
    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    self.assertEquals(resp.type, ControlMessage.OK)
    # Wait for the topology to start running; there should be one task on the
    # second worker and none on the first
    time.sleep(1)
    self.assertEquals(len(worker2.tasks), 1)
    self.assertEquals(len(worker1.tasks), 0)
    self.assertEquals(len(self.controller.computations), 1)
    
    req = ControlMessage()
    req.type = ControlMessage.STOP_COMPUTATION
    req.comp_to_stop = int(1)    
    self.controller.stop_computation(resp, req) 
    self.assertEquals(len(self.controller.computations), 0)
    cworker2 = self.controller.workers[worker2.controllerConn.getsockname()]
    self.assertTrue(cworker2 is not None)
    
    self.assertEquals(len(cworker2.assignments), 0)
    
    
    print "stopping workers"
    worker1.stop()
    worker2.stop()


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
