import unittest

from controller import *
from worker import *
from computation_state import *
from query_planner import QueryPlanner

from generic_netinterface import JSClient
import query_graph as jsapi
from jetstream_types_pb2 import *


class TestFailRecovery(unittest.TestCase):


  def test_1node_failure(self):
    dummyNode = ("host",123)
    c = Controller( ("",0) )
    c.start_computation_async = lambda x: 0  #stub out
    
    heartbeat = Heartbeat()
    heartbeat.freemem_mb = 3900
    heartbeat.cpuload_pct = 90
    heartbeat.dataplane_addr.address = dummyNode[0]
    heartbeat.dataplane_addr.portno = dummyNode[1]
    c.handle_heartbeat( heartbeat, dummyNode)

    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
    qGraph.connect(reader, cube)

    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter)

    resp = ControlMessage()
    c.handle_alter(resp, req.alter)
    if resp.type != ControlMessage.OK:
      print resp.error_msg.msg
    self.assertEquals(ControlMessage.OK, resp.type)

    req.type=ControlMessage.ALTER_RESPONSE
    c.handle_alter_response(req.alter, dummyNode)

    print c.cube_locations
    self.assertTrue('local_results' in c.cube_locations)



if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
