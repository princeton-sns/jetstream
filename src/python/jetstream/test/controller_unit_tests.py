import unittest

from controller import *
from worker import *
from computation_state import *
from query_planner import QueryPlanner

from generic_netinterface import JSClient
import operator_graph as jsapi


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

    planner = QueryPlanner([])
    # No operators
    err = planner.validate_raw_topo(alter)
    self.assertTrue( len(err) > 0) #error should be because topo is empty
    self.assertTrue( "no operators" in err)
    
    # Bad cube name
    newOp = alter.toStart.add()
    newOp.op_typename = "not a real name"
    newOp.id.computationID = compID
    newOp.id.task = 1

    new_cube = alter.toCreate.add()
    new_cube.name = "name with space"
    
    err = planner.validate_raw_topo(alter)
    print "\nError message texts:"
    self.assertTrue( len(err) > 0) #error should be because name is invalid
    print err
    self.assertTrue( "invalid cube name" in err)
    
    # Cube without aggregates
    new_cube.name = "valid_name"
    err = planner.validate_raw_topo(alter)
    
    self.assertTrue( len(err) > 0) #error should be because no aggregates
    print err
    self.assertTrue( "aggregate" in err)

    # Cube without dimension
    agg = new_cube.schema.aggregates.add()
    agg.name = "count"
    agg.type = "count"

    err = planner.validate_raw_topo(alter)
    
    self.assertTrue( len(err) > 0) #error should be because no dimensions
    print err
    self.assertTrue( "dimension" in err)
    
    # Valid cube
    dim = new_cube.schema.dimensions.add()
    dim.name = "count"
    dim.type = Element.INT32
    err = planner.validate_raw_topo(alter)
    
    self.assertEquals( len(err), 0) 
    
  def test_1node_plan(self):

    dummy_node = ("host",123)
    planner = QueryPlanner([dummy_node])

    op_graph = jsapi.OperatorGraph()
    reader = jsapi.FileRead(op_graph, "file name")
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    op_graph.add_to_PB(req.alter)

    err = planner.take_raw(req.alter)
    self.assertEquals( len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(  len(plan[dummy_node].operators), 1)

  def test_2node_plan(self):

    dummy_node = ("host",123)
    planner = QueryPlanner([dummy_node])

    op_graph = jsapi.OperatorGraph()
    reader = jsapi.FileRead(op_graph, "file name")
    cube = op_graph.cube("local_results")
    cube.add_dim("hostname", Element.STRING)
    cube.add_agg("count", jsapi.Cube.COUNT)
    cube.set_overwrite(True)  #fresh results
  
    op_graph.connect(reader, cube)
    
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    op_graph.add_to_PB(req.alter)

    err = planner.take_raw(req.alter)
    self.assertEquals( len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummy_node in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(  len(plan[dummy_node].operators), 1)
    self.assertEquals(  len(plan[dummy_node].cubes), 1)
        
    pb_to_node = plan[dummy_node].get_pb()
    self.assertEquals(  len(pb_to_node.alter.edges), 1)

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
