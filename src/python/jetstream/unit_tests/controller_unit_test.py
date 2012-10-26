import unittest

from controller import *
from worker import *
from computation_state import *
from query_planner import QueryPlanner

from generic_netinterface import JSClient
import query_graph as jsapi
from jetstream_types_pb2 import *


class TestQueryPlanner(unittest.TestCase):

#  def setUp(self):
#    self.controller = Controller(('localhost', 0))


#  def tearDown(self):
#    self.controller.stop()


  def test_bad_topos(self):
    compID = 17

    alter = AlterTopo()
    alter.computationID = compID
    planner = QueryPlanner({})
    
    # No operators
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("no operators" in err)

    # Duplicate operators
    newOp = alter.toStart.add()
    newOp.op_typename = "not a real name"
    newOp.id.computationID = compID
    newOp.id.task = 1
    alter.toStart.extend([newOp])
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("operator" in err) and ("more than once" in err))
    
    # Bad cube name
    del alter.toStart[:]
    alter.toStart.extend([newOp])
    newCube = alter.toCreate.add()
    newCube.name = "name with space"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("invalid cube name" in err)
    
    # Cube without aggregates
    newCube.name = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("aggregate" in err)

    # Cube without dimension
    agg = newCube.schema.aggregates.add()
    agg.name = "count"
    agg.type = "count"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("dimension" in err)

    # Duplicate cubes
    dim = newCube.schema.dimensions.add()
    dim.name = "count"
    dim.type = Element.INT32
    alter.toCreate.extend([newCube])
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("cube" in err) and ("more than once" in err))

    # Edge with two sources/dests
    del alter.toCreate[:]
    alter.toCreate.extend([newCube])
    newEdge = alter.edges.add()
    newEdge.src = 1
    newEdge.src_cube = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("source" in err) and ("not both" in err))
    newEdge.ClearField("src")
    newEdge.dest = 1
    newEdge.dest_cube = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("destination" in err) and ("not both" in err))
    newEdge.ClearField("dest_cube")

    # Edge with invalid source/dest
    newEdge.src_cube = "valid_name_2"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("source" in err) and ("defined" in err))
    newEdge.src_cube = "valid_name"
    newEdge.dest = 2
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("destination" in err) and ("defined" in err))
    newEdge.dest = 1

    # External edge with dest operator/cube
    newEdge.dest_addr.address = "myhost"
    newEdge.dest_addr.portno = 0
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("external" in err) and ("destination" in err))

    # Valid
    newEdge.ClearField("dest_addr")
    err = planner.validate_raw_topo(alter).lower()
    self.assertEquals(len(err), 0) 

    
  def test_1node_plan(self):

    dummy_node = ("host",123)
    planner = QueryPlanner( {dummy_node:dummy_node} )

    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter)

    err = planner.take_raw_topo(req.alter).lower()
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummy_node].operators), 1)

  def test_2node_plan(self):

    dummy_node = ("host",123)
    planner = QueryPlanner( {dummy_node:dummy_node} )

    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
    cube.set_overwrite(True)  #fresh results
  
    qGraph.connect(reader, cube)
    
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter)

    err = planner.take_raw_topo(req.alter).lower()
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummy_node].operators), 1)
    self.assertEquals(len(plan[dummy_node].cubes), 1)
        
    pb_to_node = plan[dummy_node].get_pb()
    self.assertEquals(len(pb_to_node.alter.edges), 1)

  def test_external_edge_plan(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter)
    
    MY_PORTNO = 1000
    e = req.alter.edges.add()
    e.src = req.alter.toStart[0].id.task
    e.computation = 0
    e.dest_addr.address = "myhost"
    e.dest_addr.portno = MY_PORTNO

    dummy_node = ("host",123)
    planner = QueryPlanner( {dummy_node:dummy_node} )
    err = planner.take_raw_topo(req.alter).lower()
    print err
    self.assertEquals(len(err), 0)  
    plan = planner.get_assignments(1)
    
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummy_node].operators), 1)
        
    pb_to_node = plan[dummy_node].get_pb()
    self.assertEquals(len(pb_to_node.alter.edges), 1)
    self.assertEquals(pb_to_node.alter.edges[0].dest_addr.portno, MY_PORTNO)

  def test_with_subscriber(self):
    dummy_node = ("host",123)
    planner = QueryPlanner( {dummy_node:dummy_node} )

    qGraph = jsapi.QueryGraph()
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_dim("time", Element.TIME, 1)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
    cube.set_overwrite(True)  #fresh results
 
    subscriber = jsapi.TimeSubscriber(qGraph, {"hostname":"http://foo.com"}, 1000)
    qGraph.connect(cube, subscriber)
    
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter)

    err = planner.take_raw_topo(req.alter).lower()
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    #print plan[dummy_node].get_pb()
    

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
