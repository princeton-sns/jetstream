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
from generic_netinterface import JSClient
from jetstream_types_pb2 import *

class TestOpIntegration(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))
    self.controller.start()
    print "controller bound to %s:%d" % self.controller.address
    self.client = JSClient(self.controller.address)
    self.workers = list()


  def tearDown(self):
    self.client.close()
    self.controller.stop()
    map(lambda proc: os.killpg(proc.pid, signal.SIGTERM), self.workers)
    self.workers = list()


  def test_operator(self):
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    jsnode_cmd = "./jsnoded -a localhost:%d --start -C ./config/datanode.conf" % (self.controller.address[1])
    print "starting",jsnode_cmd
    workerProc = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
    self.workers.append(workerProc)
    time.sleep(2)

    # Tell the controller to deploy a topology (it will be deployed on the only worker)
    compID = 17
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    req.alter.computationID = compID
    newTask = req.alter.toStart.add()
    newTask.op_typename = "DummyReceiver"
    newTask.id.computationID = req.alter.computationID
    newTask.id.task = 2
    
    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    self.assertEquals(resp.type, ControlMessage.OK)
    # Make sure the controller created state for this computation
    self.assertEquals(len(self.controller.computations), 1)
    # Wait for the topology to start running on the worker
    time.sleep(2)
    workerList = self.controller.get_nodes()
    assert(len(workerList) == 1)
    self.assertEquals(len(workerList[0].assignments), 1)
    self.assertEquals(workerList[0].assignments.values()[0].state, WorkerAssignment.RUNNING)


  def test_multiple_operators(self):
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    jsnode_cmd = "./jsnoded -a localhost:%d --start -C ./config/datanode.conf" % (self.controller.address[1])
    print "starting",jsnode_cmd
    workerProc = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
    self.workers.append(workerProc)
    time.sleep(2)

    # Tell the controller to deploy multiple single-node topologies (they will all be
    # deployed on the only worker)
    numComps = 10
    req = ControlMessage()
    resp = ControlMessage()
    for compID in range(1, numComps + 1):
      req.type = ControlMessage.ALTER
      req.alter.Clear()
      req.alter.computationID = compID
      newTask = req.alter.toStart.add()
      newTask.id.computationID = compID
      newTask.id.task = 2
      newTask.op_typename = "StringGrep"
      opCfg = newTask.config.add()
      opCfg.opt_name = "pattern"
      opCfg.val = ".*"
      buf = self.client.do_rpc(req, True)
      resp.Clear()
      resp.ParseFromString(buf)
      self.assertEquals(resp.type, ControlMessage.OK)
      # Make sure the controller created state for each computation
      self.assertEquals(len(self.controller.computations), compID)
    
    # Wait for the topologies to start running on the worker
    time.sleep(2)
    workerList = self.controller.get_nodes()
    assert(len(workerList) == 1)
    for assignment in workerList[0].assignments.values():
      self.assertEquals(assignment.state, WorkerAssignment.RUNNING)

 
  def test_operator_chain(self):
    # Use at least 2 workers
    numWorkers = 5
    webPortMin = 8082
    workerProcs = []
    webPort = webPortMin
    for i in range(numWorkers):
      # Create a worker 
      jsnode_cmd = "./jsnoded -a localhost:%d -w %d --start -C ./config/datanode.conf" % (self.controller.address[1], webPort)
      webPort += 1
      print "starting",jsnode_cmd
      workerProcs.append(subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid))
    self.workers.extend(workerProcs)
    # Give the workers time to register with the controller
    time.sleep(3)
    
    # Get the list of workers
    req = ControlMessage()
    req.type = ControlMessage.GET_NODE_LIST_REQ
    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    workersEp = resp.nodes
    assert(len(workersEp) == numWorkers)
  
    # Issue a query that runs an operator on each worker: send some tuples from a
    # source, filter them through the remaining workers and collect at the end.
    req = ControlMessage()
    assignedOps = []
    compID = 17
    # Make this number unique so we can check it later
    numTuples = 193
    for i in range(len(workersEp)):
      req.type = ControlMessage.ALTER
      req.alter.computationID = compID
      task = req.alter.toStart.add()
      task.id.computationID = compID
      task.id.task = i + 1  # start task numbers at 1
      # Pin the operator to the current worker
      task.site.address = workersEp[i].address
      task.site.portno = workersEp[i].portno
  
      if i == 0:
        # Send some tuples from the first worker; use a continuous sender so the chain
        # doesn't get torn down.
        task.op_typename = "ContinuousSendK"
        opCfg = task.config.add()
        opCfg.opt_name = "k"
        opCfg.val = str(numTuples)
        opCfg = task.config.add()
        opCfg.opt_name = "period"
        # Use a large period so we only have to check for one batch of tuples
        opCfg.val = str(10000)
      elif i == len(workersEp) - 1:
        # Collect tuples at the last worker
        task.op_typename = "DummyReceiver"
      else:
        # Insert no-op filters in between
        task.op_typename = "StringGrep"
        opCfg = task.config.add()
        opCfg.opt_name = "pattern"
        opCfg.val = ".*"
        opCfg2 = task.config.add()
        opCfg2.opt_name = "id"
        opCfg2.val = "0"
  
      assignedOps.append(task)
  
      if i > 0:
        # Create an edge from the previous operator to this one
        e = req.alter.edges.add()
        e.src = task.id.task - 1
        e.dest = task.id.task
        e.computation = compID
  
    # Deploy the query
    buf = self.client.do_rpc(req, True)
    resp = ControlMessage()
    resp.ParseFromString(buf)
    assert(resp.type == ControlMessage.OK)
    # The controller overwrites the computation ID, so get it again
    assert(len(self.controller.computations) == 1)
    compID = self.controller.computations.keys()[0]
    time.sleep(2)

    # Make sure the operators were started, one per worker
    webPort = webPortMin
    for i in range(len(workersEp)):
      # GET the web interface of each worker
      url = "http://" + workersEp[i].address + ":" + str(webPort) + "/"
      getResp = urllib2.urlopen(url).read()
      print getResp
      # Figure out which operator is on this worker based on identifying information
      for op in assignedOps[:]:
        opName = op.op_typename
        opId = "(" + str(compID) + "," + str(op.id.task) + ")"
        #print "SEARCHING FOR " + opName + " AND " + str(opId)
        if (opName in getResp) and (opId in getResp):
          assignedOps.remove(op)
          # If this is the final receiver, check the received tuple count
          if opName == "DummyReceiver":
            assert(str(numTuples) in getResp)
          break
      webPort += 1
    # We should have matched all operators, one per worker
    print "length at end is " + str(len(assignedOps))
    assert(len(assignedOps) == 0)


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
