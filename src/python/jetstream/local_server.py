from jetstream.server import Server
from jetstream.gen.jetstream_types_pb2 import *
import socket

DEFAULT_PORTNO = 12345
class LocalServer(Server):
  """Represents a local execution environment"""
  
  
  def __init__(self):
    n_id = NodeID()
    n_id.portno = DEFAULT_PORTNO
    n_id.address = socket.gethostbyname(socket.gethostname())
    self.thisNode = n_id
    
  def all_nodes(self):
    return [self.thisNode]  #Should be a 'local' node
    
  def get_a_node(self):
    return self.thisNode
  
  def deploy(self, op_graph):
    """Deploys an operator graph"""
    raise "Unimplemented!"
  
  