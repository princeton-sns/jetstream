import random
import socket
import time
import unittest

from local_server import LocalServer
from operator_graph import OperatorGraph,Operators
from jetstream_types_pb2 import *


class TestLocalServer(unittest.TestCase):

  def setUp(self):
    self.server = LocalServer()
    
  def test_get_nodes(self):
    nodes = self.server.all_nodes()
    self.assertEquals(len(nodes), 1)
    
    node = self.server.get_a_node()
    self.assertEquals(str(node), str(nodes[0]))
    
    node_as_str = str(node)
    self.assertTrue(str(LocalServer.DEFAULT_PORTNO) in node_as_str )
    my_hostname =  "localhost" #socket.gethostbyname(socket.gethostname())
    self.assertTrue(my_hostname in node_as_str )


  def test_op_graph(self):
    
    n = self.server.get_a_node()
    g = OperatorGraph()
    op = g.operator(Operators.UNIX, "cat /etc/shells")
    cube = g.cube("storeddata", "presumbably a schema goes here")
    g.connect(op, cube)
    cube.instantiate_on(n)
    self.assertTrue( cube.get_name().endswith("/storeddata"))
    
    self.assertTrue(cube.is_placed())
    self.assertEquals(cube.location(), n)
    self.assertEquals(op.location(), n)
    
    self.server.deploy(g)
    time.sleep(5)
    
    cube_data = self.server.get_cube(cube.get_name())
    self.assertTrue(len(cube_data) > 4)


  def test_clone_back(self):
    g = OperatorGraph()
    op = g.operator(Operators.UNIX, "cat /etc/shells")
    cube = g.cube("storeddata", "presumbably a schema goes here")
    g.connect(op, cube)
    self.assertEquals(g.opID, 3)
    g.clone_back_from(cube, 1)
    self.assertEquals(g.opID, 5)
    self.assertEquals( len(cube.preds), 1)
    self.assertEquals( len(g.edges), 2)

    self.assertTrue( (1,2) in  g.edges)
    self.assertTrue( (3,4) in  g.edges  or  (4,3) in  g.edges)

    

  def test_multi_place(self):
    n = self.server.get_a_node()
    
    n2 = NodeID()
    n2.portno =  123
    n2.address = "dummy host"
    
    g = OperatorGraph()
    op = g.operator(Operators.UNIX, "cat /etc/shells")
    cube = g.cube("storeddata", "presumbably a schema goes here")
    g.connect(op, cube)
    
    cube.instantiate_on([n, n2])
    
    
  
  

if __name__ == '__main__':
    unittest.main()
