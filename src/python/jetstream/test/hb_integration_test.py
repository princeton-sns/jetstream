#
# Integration tests spanning the python client/controller and C++ dataplane. These 
# tests create a python controller, start one or more C++ and/or python workers,
# and verify that requests (heartbeats, queries) are handled properly.
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


  def tearDown(self):
    self.controller.stop()


  def test_heartbeat(self):
    # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
    jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (self.controller.address[1])
    print "starting",jsnode_cmd
    cli_proc = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid) 
    time.sleep(2)
    self.assertEquals(len(self.controller.get_nodes()), 1)
    os.killpg(cli_proc.pid, signal.SIGTERM)


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
