import unittest

import re
import sys
from jetstream_types_pb2 import *
import regions

class TestRegions(unittest.TestCase):

  def test_get1(self):
    r = {'local_dom': re.compile('192\.168\.*'), 'localhost': re.compile('127\.0\.0\.1')} 

    nodes = []
    for ip,port in [('192.168.2.1', 12345), ('192.168.2.2', 12345), ('8.8.8.8', 12345)]:
      n = NodeID()
      n.address = ip
      n.portno = port
      nodes.append(n)

    n = regions.get_1_from_region(r['local_dom'], nodes)
    self.assertTrue(n.address.startswith("192.168.2"))
    


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
