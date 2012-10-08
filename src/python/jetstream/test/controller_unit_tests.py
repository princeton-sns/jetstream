import unittest

from controller import *
from worker import *
from computation_state import *
from query_planner import QueryPlanner

from generic_netinterface import JSClient

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
    
    self.assertTrue( len(err) > 0) #error should be because no aggregates
    print err
    self.assertTrue( "dimension" in err)
    
    # Valid cube
    dim = new_cube.schema.dimensions.add()
    dim.name = "count"
    dim.type = Element.INT32
    err = planner.validate_raw_topo(alter)
    
    self.assertEquals( len(err), 0) #error should be because no aggregates
    
    

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
