# Tests that heartbeats from the C++ jetstream client get handled correctly at the 
# controller. This test creates a mock controller, starts the js process, and verifies
# that the heartbeats arrive.


import random
import socket
import struct
import subprocess
import thread
import time
import unittest

from controller import *

from future_js_pb2 import *


class TestController(unittest.TestCase):

  def setUp(self):
    self.server = Controller(('localhost', 0))
    self.server.start_as_thread()
    print "server bound to %s:%d" % self.server.address

  def tearDown(self):
    self.server.stop()
    self.cli_proc.terminate()

  def test(self):
    jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (self.server.address[1])
    print "starting",jsnode_cmd
    self.cli_proc = subprocess.Popen(jsnode_cmd, shell=True) #stdout= subprocess.PIPE, 
    time.sleep(2)
    self.assertEquals( len(self.server.get_nodes()), 1)
    


def run_cmd(self):
  # TODO create stderr slurper
  while p.returncode is None:
    for ln in p.stdout.readlines():
      print ln
    p.poll()



if __name__ == '__main__':
  unittest.main()
  sys.exit(0)