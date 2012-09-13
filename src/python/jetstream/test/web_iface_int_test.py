#
# Integration tests spanning the python client/controller and C++ dataplane. These 
# tests create a python controller, start one or more C++ and/or python workers,
# and verify that requests (heartbeats, queries) are handled properly. By placing a
# python worker last in the operator chain, we can verify the final results locally
# (instead of having to communicate with the C++ worker processes).
#

import random
import socket
import struct
import os
import signal
import subprocess
import thread
import time
import unittest

from controller import *
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


  def test_operator(self):
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (self.controller.address[1])
    print "starting",jsnode_cmd
    workerProc = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
    time.sleep(2)

    # Tell the controller to deploy a topology (it will be deployed on the only worker)
    compID = 17
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    req.alter.computationID = compID
    
    newTask = req.alter.toStart.add()
    newTask.op_typename = "SendK"
    newTask.id.computationID = req.alter.computationID
    newTask.id.task = 2
    
    newCube = req.alter.toCreate.add()
    newCube.name = "a test cube"
    newCube.schema.name = newCube.name
    d = newCube.schema.dimensions.add()
    d.name = "text"
    d.type = Element.STRING
    
    edge = req.alter.edges.add()
    edge.src = 2
    edge.computation = compID
    edge.cube_name = "a test cube"
    
    print str(req)
    
    buf = self.client.do_rpc(req, True)
    
    resp = ControlMessage()
    resp.ParseFromString(buf)
    self.assertEquals(resp.type, ControlMessage.OK)
    # Make sure the controller created state for this computation
    self.assertTrue(compID in self.controller.computations)
    # Wait for the topology to start running on the worker
    time.sleep(2)
    workerList = self.controller.get_nodes()
    assert(len(workerList) == 1)
    self.assertEquals(workerList[0].assignments[compID].state, WorkerAssignment.RUNNING)
    time.sleep(200000)
#    os.killpg(workerProc.pid, signal.SIGTERM)



if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
