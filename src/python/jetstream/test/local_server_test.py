import unittest
import random
from jetstream.local_server import LocalServer

class TestLocalServer(unittest.TestCase):

  def setUp(self):
    self.server = LocalServer()
    
  def test_get_nodes(self):
    nodes = self.server.all_nodes()
    self.assertEquals(len(nodes), 1)
    
    node = self.server.get_a_node()
    self.assertEquals(str(node), str(nodes[0]))
    
    print nodes[0]
    pass
    
    

if __name__ == '__main__':
    unittest.main()
