import unittest



from controller import *
from worker import *
from computation_state import *
from generic_netinterface import JSClient

from jetstream_types_pb2 import *

class TestController(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))


  def tearDown(self):
    self.controller.stop()


  def test_bad_topos(self):
    compID = 17

    alter = AlterTopo()
    alter.computationID = compID

    ######  No operators
    err = self.controller.validate_topo(alter)
    self.assertTrue( len(err) > 0) #error should be because topo is empty
    self.assertTrue( "no operators" in err)
    
    ######  Name in use
    newOp = alter.toStart.add()
    newOp.op_typename = "not a real name"
    newOp.id.computationID = compID
    newOp.id.task = 1

    self.controller.computations[compID] = "in use"
    err = self.controller.validate_topo(alter)
    self.assertTrue( len(err) > 0) #error should be because compID is in use
    self.assertTrue( "in use" in err)
    del self.controller.computations[compID]


    ######  Bad cube name
    new_cube = alter.toCreate.add()
    new_cube.name = "name with space"
    
    err = self.controller.validate_topo(alter)
    
    self.assertTrue( len(err) > 0) #error should be because compID is in use
    self.assertTrue( "invalid cube name" in err)
    
    
    

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)