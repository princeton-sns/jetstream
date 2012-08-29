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
    cli_proc = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
    time.sleep(2)

    # Tell the controller to deploy a topology (it will deploy it on the only worker)
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    newTask = req.alter.toStart.add()
    newTask.op_typename = "FileRead"
    newTask.id.computationID = 17
    newTask.id.task = 2
    configEntry = TaskMeta.DictEntry()
    configEntry = newTask.config.add()
    configEntry.opt_name = "file"
    #TODO: Need a better way to construct relative paths
    configEntry.val = "../../src/tests/data/base_operators_data.txt";
    
    buf = self.client.do_rpc(req, True)
    print "deploy finished..."
    # Wait for the topology to start running on the worker
    time.sleep(2)
    #TODO: Create state in controller and assert that its there
    os.killpg(cli_proc.pid, signal.SIGTERM)
 
  def test_operator_chain(self):
    pass


def run_cmd(self):
  # TODO create stderr slurper
  while p.returncode is None:
    for ln in p.stdout.readlines():
      print ln
    p.poll()


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
