import socket
import subprocess
import threading

from jetstream.server import Server
from jetstream.gen.jetstream_types_pb2 import *

from operator_graph import *

class Controller(Server):
  """Represents a stand-alone controller."""
  DEFAULT_PORTNO = 12345
  
  def __init__(self):
    n_id = NodeID()
    n_id.portno = self.DEFAULT_PORTNO
    n_id.address = socket.gethostbyname(socket.gethostname())
    self.thisNode = n_id
    self.cubes = {}

  def get_nodes(self):
    return []
    
  def deploy(self, op_graph):
    """Deploys an operator graph"""
    
    dests = {}
    for cube in op_graph.cubes:
      c = self.instantiate_cube(cube)
      dests[cube.get_id()] = c
      self.cubes[cube.get_name()] = c
    for op in op_graph.operators:
      dests[op.get_id()] = self.instantiate_op(op)
    
    for (e1, e2) in op_graph.edges:
      d1, d2 = dests[e1], dests[e2]
      d1.add_dest(d2)
    
    for op in dests.values():
      if isinstance(op, Operator):
        op.start()
