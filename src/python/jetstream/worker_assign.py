
import types

from jetstream_types_pb2 import *


class WorkerAssignment (object):
  """An assignment of operators/cubes to a worker for a given computation.
  Internally, a list of operators, cubes, and edges."""
  
  # States of an assignment
  RUNNING = 1
  STOPPED = 2
  
  def __init__ (self, compID, operators=None, cubes=None):
    self.compID = compID
    self.state = WorkerAssignment.STOPPED
      #lists of protobuf items
    self.operators = operators if operators is not None else []
    self.cubes = cubes if cubes is not None else []
    
    self.edges = [] #just a list of PB edges for now
    self.policies = [] # list of PB policies
    
  def __eq__ (self, other):
    if isinstance(other, WorkerAssignment):
      if ((len(self.operators) != len(other.operators)) or
          (len(self.cubes) != len(other.cubes)) or
          (self.compID != other.compID)):
        return False
      # Compare the list of operators and cubes (assume they are sorted for now)
      for i in range(len(self.operators)):
        if self.operators[i].id.task != other.operators[i].id.task:
          return False
      for i in range(len(self.cubes)):
        if self.cubes[i].name != other.cubes[i].name:
          return False
      return True
    return NotImplemented


  def __ne__ (self, other):
    result = self.__eq__(other)
    if result is NotImplemented:
      return result
    return not result


  def add_node (self, node):
    if isinstance(node, TaskMeta):
      self.operators.append(node)
    else:
      assert(isinstance(node, CubeMeta))
      self.cubes.append(node)

  def add_edge(self, e):
    self.edges.append(e)

  def add_policy(self, p):
    self.policies.append(p)

  def get_pb(self):
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    self.fillin_alter(req.alter.add())
    return req


  def fillin_alter(self, alter):
    alter.computationID = self.compID
    alter.toStart.extend(self.operators)
    alter.toCreate.extend(self.cubes)
    alter.edges.extend(self.edges)
    alter.congest_policies.extend(self.policies)
    
    
