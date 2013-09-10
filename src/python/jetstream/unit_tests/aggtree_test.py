#
# Tests the JetStream graph interface in jsgraph.py. The graphs created here consist
# of minimally defined operators and cubes. They should not be used as real queries
# submitted to a controller.
#

import random
import os
import signal
import subprocess
import thread
import time
import unittest

from jetstream_types_pb2 import *
from query_planner import QueryPlanner
import query_graph as jsapi


class TestAggTree (unittest.TestCase):


  def test_2node (self):
    dummyNode, dummy2 = ("host",123), ("host2", 123)
    planner = QueryPlanner( {dummyNode:dummyNode, dummy2:dummy2} )

    g = jsapi.QueryGraph()
    
    ucube = g.add_cube("union")
    ucube.add_dim("state", Element.STRING, 0)
    ucube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)

    readers = []
    for node in [dummyNode, dummy2]:
      reader = jsapi.FileRead(g, "file name")
      readers.append(reader)
      nID = NodeID()
      nID.address, nID.portno = node
      reader.instantiate_on(nID)
#      g.connect(reader, ucube) #agg tree test, so we don't need this

    g.agg_tree(readers, ucube)
    self.assertEquals(6, len(g.edges))  # 2x (op --> cube --> subscriber --> union)

    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    g.add_to_PB(req.alter.add())

    err = planner.take_raw_topo(req.alter[0]).lower()
    self.assertEquals(len(err), 0)

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
