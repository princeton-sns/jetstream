from jetstream_types_pb2 import *


# Enumeration of already-defined operators
class Operators(object):

  UNIX = "Unix"
  Fetcher = "Fetcher"
  

class OperatorGraph(object):

  def __init__(self):
    self.opID = 1  #should be the NEXT ID to hand out
    self.edges = set([]) #pairs of opIDs
    self.operators = {} # maps ID to value
    self.cubes = {}  #id to value


  def operator(self, type, cfg):
    """Add an operator to the graph"""
    o = Operator(self, type, cfg, self.opID)
    self.operators[self.opID] = o
    self.opID += 1
    return o  
    
  def cube(self, name, desc = {}):
    """Add a cube to the graph"""
    o = Cube(self, name, desc, self.opID)
    self.cubes[self.opID] = o
    self.opID += 1
    return o

    
  def add_to_PB(self, alter):
    for id,operator in self.operators.items():
      operator.add_to_PB(alter)
    for id,cube in self.cubes.items():
      cube.add_to_PB(alter)
    for e in self.edges:
      pb_e = alter.edges.add()
      pb_e.computation = 0
      if e[0] in self.operators:
        pb_e.src = e[0]
        if e[1] in self.operators:
          pb_e.dest = e[1]
        else:
          assert(e[1] in self.cubes)
          pb_e.cube_name = self.cubes[e[1]].name
      else:
        raise "haven't implemented out-edges from cubes"
    
  def connect(self, oper1, oper2):
    self.edges.add( (oper1.get_id(), oper2.get_id()) )
    oper2.add_pred(oper1)

    
  def clone_back_from(self, head, numcopies):
    to_copy = {}  #maps id to object
    to_inspect = set([head])
    edges_to_copy = []
    while len(to_inspect) > 0:
      e = to_inspect.pop()
      to_copy[e.get_id()] = e
      for pred in e.preds:
        edges_to_copy.append( (pred.get_id(), e.get_id()) )
        if pred not in to_copy and pred not in to_inspect:
          to_inspect.add(pred)
    #at this point we have a set of nodes to inspect, with their IDs
    id_to_obj = {}
    newheads = []
    for n in range(numcopies):    
      old_to_new_ids = {}
      for id,dest in to_copy.items():
        new_dest = self.copy_dest(dest)
        id_to_obj[new_dest.get_id()] = new_dest
        old_to_new_ids[id] = new_dest.get_id()
      for (e1,e2) in edges_to_copy:
        self.connect( id_to_obj[old_to_new_ids[e1]] , id_to_obj[old_to_new_ids[e2]])
      new_head =  id_to_obj[ old_to_new_ids[head.get_id()]]
      newheads.append(new_head)
    return newheads


  def copy_dest(self, dest):
    if isinstance(dest,Operator):
      new_cfg = {}
      new_cfg.update(dest.cfg)
      return self.operator(dest.type, new_cfg)
    elif isinstance(dest,Cube):
      return self.cube(dest.name, dest.desc)
    else:
      raise "unexpected param to copy_dest"

      
## These represent the abstract concept of an operator or cube, for building
# the operator graphs. The concrete executable implementations are elsewhere.
class Destination(object):
  
  def __init__(self, graph, id):
    self.preds = set()
    self.graph = graph  #keep link to parent OperatorGraph
    self.id = id
    self._location = None

  
  def add_pred(self, p):
    self.preds.add(p)
  
  def get_id(self):
    return self.id
    
  def location(self):
    return self._location
    
  def is_placed(self):
    return self._location is not None    

  def instantiate_on(self, n):
   
    if isinstance(n, NodeID):
      self._location = n
      for p in self.preds:
        if not p.is_placed():
          p.instantiate_on(n)
    else:
      headcopies = self.graph.clone_back_from(self, len(n) -1 )
      for site,copy in zip(n[1:], headcopies):
        copy.instantiate_on(site)
      self.instantiate_on(n[0])


class Operator(Destination):
  
  def __init__(self, graph, type, cfg, id):
    super(Operator,self).__init__(graph, id)
    self.type = type
    self.cfg = cfg # should be a map
    

  def add_to_PB(self, alter):
     task_meta = alter.toStart.add()
     task_meta.id.computationID = 0 #filled in by controller
     task_meta.id.task = self.id
     task_meta.op_typename = self.type
     if self._location is not None:
       task_meta.site.CopyFrom(self._location)
     for opt,val in self.cfg.items():
       d_entry = task_meta.config.add()
       d_entry.opt_name = opt
       d_entry.val = val
     

class Cube(Destination):
  
  STRING = "string"
  COUNT = "count"

  def __init__(self, graph, name, desc, id):
    super(Cube,self).__init__(graph, id)
    self.name = name
    self.desc = {}
    self.desc.update(desc)
    if 'dims' not in self.desc:
      self.desc['dims'] = []
    if 'aggs' not in self.desc:
      self.desc['aggs'] = []


  def add_dim(self, dim_name, dim_type):
    self.desc['dims'].append(  (dim_name, dim_type) )

  def add_agg(self, a_name, a_type):
    self.desc['aggs'].append(  (a_name, a_type) )

  def add_to_PB(self, alter):
    c_meta = alter.toCreate.add()
    c_meta.name = self.name
    if self._location is not None:
      c_meta.site.CopyFrom(self._location)
    
    for (name,type) in self.desc['dims']:    
      d = c_meta.schema.dimensions.add()
      d.name = name
      d.type = type
    for (name,type) in self.desc['aggs']:    
      d = c_meta.schema.aggregates.add()
      d.name = name
      d.type = type
      
     
  def get_name(self):
    if self._location is not None:
      return  "%s:%d/%s"% (self._location.address, self._location.portno, self.name)
    else:
      return self.name


def FileRead(graph, file):
   cfg = {"file":file}
   return graph.operator("FileRead", cfg)  
   


def StringGrep(graph, pattern):
   cfg = {"pattern":pattern}
   return graph.operator("StringGrep", cfg)  