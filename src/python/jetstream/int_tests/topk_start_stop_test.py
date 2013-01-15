#
# Integration tests spanning the python client/controller and C++ dataplane. These 
# tests create a python controller, start one or more C++ and/or python workers,
# and verify that operators and operator chains are handled properly. By placing a
# python worker last in the operator chain, we can verify the final results locally
# (instead of having to communicate with the C++ worker processes).
#

import urllib2
import socket
import struct
import os
import signal
import subprocess
import thread
import time
import unittest

from controller import *
from remote_controller import *
from generic_netinterface import JSClient
from jetstream_types_pb2 import *
from topk import get_graph

class TestOpIntegration(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))
    self.controller.start()
    print "controller bound to %s:%d" % self.controller.address
    self.client = JSClient(self.controller.address)
    self.server = RemoteController()
    self.server.connect(self.controller.address[0], self.controller.address[1])
    self.workers = list()


  def tearDown(self):
    self.client.close()
    self.controller.stop()
    map(lambda proc: os.killpg(proc.pid, signal.SIGTERM), self.workers)
    self.workers = list()

  def start_workers(self, num_workers):
    # Use at least 2 workers
    webPortMin = 8083
    workerProcs = []
    webPort = webPortMin
    for i in range(num_workers):
      # Create a worker 
      jsnode_cmd = "./jsnoded -a localhost:%d -w %d --start -C ./config/local_bare.conf" % (self.controller.address[1], webPort)
      webPort += 1
      print "starting",jsnode_cmd
      workerProcs.append(subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid))
      time.sleep(1)

    self.workers.extend(workerProcs)
    # Give the workers time to register with the controller
    time.sleep(3)

  def verify_workers(self, num_workers):
    # Get the list of workers
    req = ControlMessage()
    req.type = ControlMessage.GET_NODE_LIST_REQ
    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    workersEp = resp.nodes
    self.assertEquals(len(workersEp), num_workers)



  def test_topk(self):
    num_workers = 2
    self.start_workers(num_workers)
    self.verify_workers(num_workers)

    root_node = self.server.get_a_node()
    assert isinstance(root_node, NodeID)
    all_nodes = self.server.all_nodes()

    for i in range(5):
      g = get_graph(root_node, all_nodes, rate=1000)
      req = g.get_deploy_pb()
      cid = self.server.deploy_pb(req)
      if type(cid) == types.IntType:
        print time.ctime(),"Computation running; ID =",cid
      else:
        print "computation failed",cid
        break
      time.sleep(3)
      workerList = self.controller.get_nodes()
      assert(len(workerList) == num_workers)
      #self.assertEquals(len(workerList[0].assignments), 1)
      for j in range(num_workers):
        self.assertEquals(workerList[j].assignments.values()[0].state, WorkerAssignment.RUNNING)
      self.server.stop_computation(cid)
      print time.ctime(),"Computation stopped; ID =",cid
      time.sleep(2)   

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
