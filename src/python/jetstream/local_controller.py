import socket
import subprocess
import threading


from controller_api import ControllerAPI
from jetstream_types_pb2 import *

from operator_graph import *

class LocalController(ControllerAPI):
  """Represents a local execution environment"""
  DEFAULT_PORTNO = 12345
  
  
  def __init__(self):
    n_id = NodeID()
    n_id.portno = self.DEFAULT_PORTNO
    n_id.address = "localhost" #socket.gethostbyname(socket.gethostname()) #not all hosts have a resolvable name for their own IP addr
    self.thisNode = n_id
    self.cubes = {}
    
  def all_nodes(self):
    return [self.thisNode]  #Should be a 'local' node
    
  def get_a_node(self):
    return self.thisNode
  
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

  def instantiate_cube(self, cube):
    return LocalCube()

  def instantiate_op(self, op):
    if op.type == Operators.UNIX:
      return LocalUnix(op.id, op.desc)
    elif op.type == Operators.Fetcher:
      the_cube = cubes[op.desc] #FIXME should be a name not an ID here?
      return LocalFetcher(self, the_cube)
    else:
      raise "Unknown type " + op.type
      
  def get_cube(self, cube_name):
    if cube_name in self.cubes:
      c = self.cubes[cube_name]
      return c.rows
    else:
      return []
 
class LocalCube(Cube):
  def __init__(self):
    self.rows = []
     
  def receive(self,r):
    self.rows.append(r)


class LocalUnix(Operator):
  def __init__(self, id, desc):
    self.id = id
    self.desc = desc
    self.t = None
    self.dests = []
  
  def add_dest(self, p):
    self.dests.append(p)
    
  def receive(self,r):
    pass #should never happen?
    
  def start(self):
    tname = "unix-runner %d" % self.id
    self.t = threading.Thread(group = None, target = self.run_cmd, name = tname, args = ( ) )
    self.t.start()

  def run_cmd(self):
    p = subprocess.Popen(self.desc, stdout= subprocess.PIPE, shell=True)
    # TODO create stderr slurper
    while p.returncode is None:
      for ln in p.stdout.readlines():
#        print "Task %d outputs line: %s" % (self.id, ln)
        for dest in self.dests:
          dest.receive(ln)
      p.poll()

class LocalFetcher(Operator):
  def __init__(self):
    self.dests = []
  
  def add_dest(self, p):
    self.dests.append(p)

  def start():
  # self.t = threading.Thread()
    pass

  def fetch_thread():
    pass
