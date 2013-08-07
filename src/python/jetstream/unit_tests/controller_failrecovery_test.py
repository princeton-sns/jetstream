import unittest

from controller import *
from worker import *
from computation_state import *
import query_planner 

from generic_netinterface import JSClient
import query_graph as jsapi
from jetstream_types_pb2 import *



def add_node(c, dummyNodeOutbound, dummyNodeListening):
  heartbeat = Heartbeat()
  heartbeat.freemem_mb = 3900
  heartbeat.cpuload_pct = 90
  heartbeat.dataplane_addr.address = dummyNodeListening[0]
  heartbeat.dataplane_addr.portno = dummyNodeListening[1]
  return c.handle_heartbeat( heartbeat, dummyNodeOutbound)


class TestFailRecovery(unittest.TestCase):


  def test_1node_failure(self):
    dummyNodeOutbound = ("host",123)
    dummyNodeListening = ("host",1235)

    c = Controller( ("",0) )
    c.start_computation_async = lambda x: 0  #stub out
    
        #Add a node
    add_node(c, dummyNodeOutbound, dummyNodeListening)

        #add a small topology
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
    qGraph.connect(reader, cube)

    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter.add())
    resp = ControlMessage()
    c.handle_alter(resp, req.alter[0])
    if resp.type != ControlMessage.OK:
      print resp.error_msg.msg
    self.assertEquals(ControlMessage.OK, resp.type)

    req.type=ControlMessage.ALTER_RESPONSE
    query_planner.overwrite_comp_ids(req.alter[0], resp.started_comp_id)
    c.handle_alter_response(req.alter[0], dummyNodeOutbound)

        #confirm topology started
    self.assertTrue('local_results' in c.cube_locations)
#    print c.cube_locations
    
    #   drop node
    c.worker_died(dummyNodeOutbound)
    self.assertTrue(c.cube_locations['local_results'] == None)
    self.assertTrue(c.pending_work[dummyNodeListening] != None)
    print c.pending_work[dummyNodeListening]
    dummyNodeOutbound = ("host", 124) #change of outgoing port    
    restart_cmd = add_node(c, dummyNodeOutbound, dummyNodeListening)
#    print "Controller handed back",restart_cmd
    self.assertEquals(ControlMessage.ALTER, restart_cmd.type)
    self.assertEquals(1, len(restart_cmd.alter))
    self.assertEquals(1, len(restart_cmd.alter[0].edges))
    self.assertEquals(1, len(restart_cmd.alter[0].toStart))


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
