# Integration tests spanning the python client/controller and C++ dataplane. These 
# tests create a python controller, start one or more C++ and/or python workers,
# and verify that requests (heartbeats, queries) are handled properly. By placing a
# python worker last in the operator chain, we can verify the final results locally
# (instead of having to communicate with the C++ worker processes).

import random
import socket
import struct
import subprocess
import thread
import time
import unittest

from controller import *

from jetstream_types_pb2 import *


class TestController(unittest.TestCase):

  def setUp(self):
    self.server = Controller(('localhost', 0))
    self.server.start_as_thread()
    print "server bound to %s:%d" % self.server.address

  def tearDown(self):
    self.server.stop()

  def test_heartbeat(self):
    jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (self.server.address[1])
    print "starting",jsnode_cmd
    cli_proc = subprocess.Popen(jsnode_cmd, shell=True) #stdout= subprocess.PIPE, 
    time.sleep(2)
    self.assertEquals( len(self.server.get_nodes()), 1)
    cli_proc.terminate()

  def test_operator(self):
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
