import types

from jetstream_types_pb2 import *


class QueryGraph(object):
  """Represents the client's-eye-view of a computation.
 We use the task IDs of operators internally. We also use the same ID space for cubes, in this class. However, the cube names are substituted in at serialization time.
 This means, in particular, that you should be able to share a cube across computations. 
"""

  def __init__(self):
    self.nID = 1          # the NEXT ID to hand out
    self.edges = set([])  # pairs of nIDs
    self.operators = {}   # maps id -> value
    self.cubes = {}       # maps id -> value


  def add_operator(self, type, cfg):
    """Add an operator to the graph"""
    o = Operator(self, type, cfg, self.nID)
    self.operators[self.nID] = o
    self.nID += 1
    return o    

  def add_existing_operator(self, o):
    id = self.nID
    self.operators[id] = o
    self.nID += 1
    return id
    
    
  def add_cube(self, name, desc = {}):
    """Add a cube to the graph"""
    c = Cube(self, name, desc, self.nID)
    self.cubes[self.nID] = c
    self.nID += 1
    return c

  def add_to_PB(self, alter):
    
    alter.computationID = 0
    for id,operator in self.operators.items():
      operator.add_to_PB(alter)
    for id,cube in self.cubes.items():
      cube.add_to_PB(alter)
    for e in self.edges:
      pb_e = alter.edges.add()
      pb_e.computation = 0
      
        
      if e[0] in self.operators:
        pb_e.src = e[0]
      else:
        pb_e.src_cube=self.cubes[e[0]].name

      if e[1] in self.operators:
        pb_e.dest = e[1]
      else:  #dest wasn't an operator, so must be cube
        if e[1] not in self.cubes:
          print "ERR: edge",e,"connects to nonexistant node"
          print "operators:",self.operators
          print "cubes:",self.cubes
        assert(e[1] in self.cubes)
        pb_e.dest_cube = self.cubes[e[1]].name
    
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
      return self.add_operator(dest.type, new_cfg)
    elif isinstance(dest,Cube):
      return self.add_cube(dest.name, dest.desc)
    else:
      raise "unexpected param to copy_dest"

      
# This represents the abstract concept of an operator or cube, for building
# the query graphs. The concrete executable implementations are elsewhere.
class Destination(object):
  
  def __init__(self, graph, id):
    self.preds = set()
    self.graph = graph  #keep link to parent QueryGraph
    self.id = id
    self._location = None

  
  def add_pred(self, p):  
    assert( isinstance(p,Destination) )

    self.preds.add(p)
  
  def get_id(self):
    return self.id
    
  def location(self):
    return self._location
    
  def is_placed(self):
    return self._location is not None    

  def instantiate_on(self, n):
    """If n is a NodeID, will specify to place this destination on that node.
    If n is a list of node IDs, will clone and place on each
    """
    if isinstance(n, NodeID):
      self._location = n
#      for p in self.preds:  
#        if not p.is_placed():
#          p.instantiate_on(n)
    else:
      #n should be a list
      assert len(n) > 0
      assert isinstance(n[0], NodeID) 
      headcopies = self.graph.clone_back_from(self, len(n) -1 )
      for site,copy in zip(n[1:], headcopies):
        copy.instantiate_on(site)
      self.instantiate_on(n[0])


class Operator(Destination):

  # Enumeration of already-defined operators
  class OpType (object):
    FILE_READ = "FileRead"
    STRING_GREP = "StringGrep"
    EXTEND = "ExtendOperator"
    NO_OP = "ExtendOperator"  # ExtendOperator without config == NoOp
    SEND_K = "SendK"
    RATE_RECEIVER = "RateRecordReceiver"
    TIME_SUBSCRIBE = "TimeBasedSubscriber"

    # Supported by Python local controller/worker only
    UNIX = "Unix"
    FETCHER = "Fetcher"

  
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
       d_entry.val = str(val)
     return task_meta
     

class Cube(Destination):

  class AggType (object):
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


  def add_dim(self, dim_name, dim_type, offset):
    self.desc['dims'].append(  (dim_name, dim_type, offset) )


  def add_agg(self, a_name, a_type, offset):
    self.desc['aggs'].append(  (a_name, a_type, offset) )

  def get_dimensions(self):
    r = {}
    for dim_name, dim_type, offset in self.desc['dims']:
      r[offset] = (dim_name, dim_type)
    return r
    
  def set_overwrite(self, overwrite):
    assert(type(overwrite) == types.BooleanType)
    self.desc['overwrite'] = overwrite


  def add_to_PB(self, alter):
    c_meta = alter.toCreate.add()
    c_meta.name = self.name
    if self._location is not None:
      c_meta.site.CopyFrom(self._location)
    
    for (name,type, offset) in self.desc['dims']:    
      d = c_meta.schema.dimensions.add()
      d.name = name
      d.type = type
      d.tuple_indexes.append(offset)
    for (name,type, offset) in self.desc['aggs']:    
      d = c_meta.schema.aggregates.add()
      d.name = name
      d.type = type
      d.tuple_indexes.append(offset)
    if 'overwrite' in  self.desc:
      c_meta.overwrite_old = self.desc['overwrite'] 

     
  def get_name(self):
    if self._location is not None:
      return  "%s:%d/%s"% (self._location.address, self._location.portno, self.name)
    else:
      return self.name


##### Useful operators #####
    
def FileRead(graph, file):
   cfg = {"file":file}
   return graph.add_operator(Operator.OpType.FILE_READ, cfg)  
   

def StringGrepOp(graph, pattern):
   cfg = {"pattern":pattern}
   return graph.add_operator(Operator.OpType.STRING_GREP, cfg)  
   
   
def ExtendOperator(graph, typeStr, fldValsList):
    cfg = {"types": typeStr}
    assert len(fldValsList) == len(typeStr) and len(typeStr) < 11
    i = 0
    for x in fldValsList:
      cfg[str(i)] = str(x)
      i += 1
    return graph.add_operator(Operator.OpType.EXTEND, cfg)
    
    
def NoOp(graph, file):
   cfg = {}
   return graph.add_operator(Operator.OpType.EXTEND, cfg)  

class TimeSubscriber(Operator):
  def __init__ (self, graph, my_filter, interval, sort_order = [], num_results = -1):
    super(TimeSubscriber,self).__init__(graph,Operator.OpType.TIME_SUBSCRIBE, {}, 0)
    self.filter = my_filter
    self.cfg["window_size"] = interval
    self.id = graph.add_existing_operator(self)
    assert self.id != 0
  

  def add_to_PB(self, alter):
    my_meta = Operator.add_to_PB(self,alter)
    
    assert( len(self.preds) == 1)
    pred_cube = list(self.preds)[0]
    #Need to convert the user-specified selection keys into positional form for the DB
    dims_by_id = pred_cube.get_dimensions()
    tuple = Tuple()
    max_dim = max(dims_by_id.keys())
    for id in range(0, max_dim+1):
      el = tuple.e.add()
      if id not in dims_by_id:
        continue
      dim_name,dim_type = dims_by_id[id]
      if dim_name in self.filter:
        if dim_type == Element.STRING:
          el.s_val = self.filter[dim_name]
        else:
          raise "Panic"
        
    #We do this in two phases
    
    serialized_filter = tuple.SerializeToString()
    self.cfg["slice_tuple"] = serialized_filter
    
    
##### Test operators #####
 
def SendK(graph, k):
   cfg = {"k":str(k)}
   return graph.add_operator(Operator.OpType.SEND_K, cfg)  


def RateRecord(graph):
   cfg = {}
   return graph.add_operator(Operator.OpType.RATE_RECEIVER, cfg)         
