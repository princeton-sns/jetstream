import types

from itertools import izip, tee

from jetstream_types_pb2 import *
Dimension = CubeSchema.Dimension
from operator_schemas import SCHEMAS, OpType,SchemaError,check_ts_field
from base_constructs import *
import regions

# from python itertools recipes
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

class QueryGraph(object):
  """Represents the client's-eye-view of a computation.

  We use the task IDs of operators internally. We also use the same numeric ID
  space for cubes, in this class. However, the cube names are substituted in at
  serialization time.
  This means, in particular, that you should be able to share a cube across
  computations.

  Note that cube names are, as a result, NOT required to be unique across a
  computation here. However the server does impose this limitation.
  """

  def __init__(self):
    self.nID = 1            # the NEXT ID to hand out
    self.edges = {}    # maps pairs of nIDs to aux info
    self.operators = {}     # maps id -> value
    self.cubes = {}         # maps id -> value
    self.externalEdges = [] # literal protobuf edges
    self.policies = []


  def get_sources(self):
    """Returns a list of IDs corresponding to nodes without an in-edge"""
    non_sources = set([x for (y,x) in self.edges.keys()])
    return [x for x in (self.operators.keys() + self.cubes.keys()) if x not in non_sources]

  def forward_edge_map(self):
    """Returns a map from each node to its set of forward edges"""
    ret = {}
    for (s,d) in self.edges.keys():
      if s not in ret:
        ret[s] = [d]
      else:
        ret[s].append(d)
    return ret

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

  def remove(self, o):  # NOTE: this routine is not yet carefully tested -- asr, 11/27/12
    if isinstance(o, int):
      oid = o
      o = self.operators[oid] if oid in self.operators else self.cubes[oid]
    else:
      oid = o.get_id()

    for p in o.preds:   #remove backward edges first
      del self.edges[ (p.id,oid) ]
    to_drop = []
    for src,dest in self.edges.keys():
      if src == oid:   
        to_drop.append (  (src,dest) )
        d = self.operators[dest] if dest in self.operators else self.cubes[dest]
        d.remove_pred(o)
    for e in to_drop:   #now remove forward edges
      del self.edges[e]

    if oid in self.operators:
      del self.operators[oid]
    else:
      del self.cubes[oid]

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
    for e in self.externalEdges:
        pb_e = alter.edges.add()
        pb_e.CopyFrom(e)
        assert pb_e.IsInitialized()
    for e,aux in self.edges.items():
      if aux.get('dummy', False):
        continue  #silently ignore
      pb_e = alter.edges.add()
      pb_e.computation = 0

      if 'max_kb_per_sec' in aux:
        pb_e.max_kb_per_sec = aux['max_kb_per_sec']
      else:
        assert(len(aux) == 0)

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
    for p in self.policies:    
      print "policy:", p
      pb_policy = alter.congest_policies.add()
      for op in p:
        pb_o = pb_policy.op.add()
        pb_o.task = op
        pb_o.computationID = 0


  def connect(self, oper1, oper2, bwLimit=-1):
    """ Add an edge from the the first operator to the second. """
    aux = {}
    if bwLimit > 0:
      aux['max_kb_per_sec'] = bwLimit
    elif bwLimit == 0:
      aux['dummy'] = True
    
    self.edges[ (oper1.get_id(), oper2.get_id()) ] = aux
    oper2.add_pred(oper1)
    return oper2

  def chain(self, operators):
    """ Add edges from each destination in the list to the next destination in
        the list. Cubes are allowed. """
    assert all(isinstance(op, Destination) for op in operators)
    for oper, next_oper in pairwise(operators):
      self.connect(oper, next_oper)
    return next_oper

  # right now this is for adding an edge to the client so it can act as a
  # receiver
  def connectExternal(self, operator, nodeid):
    """ Add an edge from an operator to a node """
    e = Edge()
    e.computation = 0  # dummy
    e.src = operator.get_id()
    e.dest_addr.CopyFrom(nodeid)
    self.externalEdges.append(e)

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

  def get_deploy_pb(self):
    self.validate_schemas()
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    self.add_to_PB(req.alter.add())
    return req

  def node_type(self, id):
    if id in self.operators:
      return self.operators[id].type
    else:
      return self.cubes[id].name + " cube"

  def is_filtering_subsc(self, o):
    return o in self.operators and self.operators[o].type == OpType.FILTER_SUBSCRIBER

  def validate_schemas(self):
    worklist = self.get_sources()  #worklist is a list of IDs
#    print "initial source operators",worklist
    input_schema = {}
    for n in worklist:
      input_schema[n] = ()

    forward_edges = self.forward_edge_map();
    for n in worklist:
      is_operator = n in self.operators
    
      # print "validating schemas for outputs of %d" % n
      # note that cubes don't have an output schema and subscribers are a special case
      if self.is_filtering_subsc(n):
        out_schema = filter_subsc_validate(self.operators[n], input_schema)
      elif is_operator:
#        print "found operator",n,"of type",self.operators[n].type
        out_schema = self.operators[n].out_schema( input_schema[n])
#        print "out schema is %s, have %d out-edges" % (str(out_schema), len(forward_edges.get(n, []) ))
      else:
        out_schema = self.cubes[n].out_schema (input_schema[n])

#      print "out-schema for", n, self.node_type(n), "is", out_schema
      for o in forward_edges.get(n, []):
        if self.is_filtering_subsc(o) and n in self.cubes:
          continue
          
        if o in input_schema: #already have a schema:
          if input_schema[o] != out_schema:
            err_msg = "Edge from %d of type %s to %d of type %s (%s) doesn't match existing schema %s" \
                % (n, self.node_type(n), o, self.node_type(o), str(out_schema), str(input_schema[o]))
            raise SchemaError(err_msg)
        else:
          input_schema[o] = out_schema
          worklist.append(o)

  def add_policy(self, oplist):
    l = []
    for op in oplist:
      l.append( op.id )
    self.policies.append(l)
    pass


###### Agg tree builder ##########


  def agg_tree(self, src_list, union_cube, region_list = None, subscriber_interval=2000):
    """Preconditions: The sources should be pinned.
   Postcondition: extra cubes have been added to build an aggregation tree
"""  
    ops = []
    for src in src_list:
      local_cube = self.copy_dest(union_cube)
      local_cube.instantiate_on(src.location())
      local_cube.name = local_cube.name + "_local"
      sub = TimeSubscriber(self, filter={}, interval=subscriber_interval)
      sub.set_cfg("window_offset", 2* 1000) #...trailing by a few
#      sub.set_cfg("simulation_rate", options.warp_factor)
#      sub.set_cfg("ts_field", 0)
#      sub.set_cfg("start_ts", options.start_ts)
        #TODO offset
      self.chain([src, local_cube, sub])
      ops.append(sub)  
  
    if region_list is not None:
      cube_in_r = {}
      for (name, defn) in region_list.items():
        node_in_r = regions.get_1_from_region(defn, all_nodes)
        if node_in_r:
          print "for region %s, aggregation is on %s:%d" % (name, node_in_r.address, node_in_r.portno)
          cube_in_r[name] = self.copy_dest(union_cube)
          cube_in_r[name].name = "partial_agg"
          cube_in_r[name].instantiate_on(node_in_r)
      for op in ops:
        rgn = regions.get_region(r_list, op.location())
        if not rgn:
          print "No region for node %s:%d" % (op.location().address, op.location().portno)
        self.connect(op, cube_in_r[rgn])

      ops = []
      for cube in cube_in_r.values():
        sub = TimeSubscriber(self, filter={}, interval=subscriber_interval)
        sub.set_cfg("window_offset", 2* 1000) #...trailing by a few
  #      sub.set_cfg("simulation_rate", options.warp_factor)
  #      sub.set_cfg("ts_field", 0)
  #      sub.set_cfg("start_ts", options.start_ts)
          #TODO offset
        g.connect(cube, sub)
        ops.append(sub)      
        
    for op in ops:
      self.connect(op, union_cube)





##### Useful operators #####

def FileRead(graph, file, skip_empty=False):
   assert isinstance(file, str)
   cfg = { "file":file, "skip_empty" : str(skip_empty)}
   return graph.add_operator(OpType.FILE_READ, cfg)


def StringGrepOp(graph, pattern):
   cfg = {"pattern":pattern, "id": 0}
   import re
   re.compile(pattern) # throw an error on bad patterns
   return graph.add_operator(OpType.STRING_GREP, cfg)

def CSVParse(graph, types, fields_to_keep='all'):
   assert isinstance(types, str)

   keepStr = fields_to_keep
   if fields_to_keep != 'all':
     if not all(isinstance(f, int) for f in fields_to_keep):
       raise SchemaError("CSVParse needs either \"all\" or " \
        "list of field indices to keep, got {0}".format(str(fields_to_keep)))
     keepStr = ' '.join(map(str, fields_to_keep))
   assert isinstance(keepStr, str)
   cfg = {"types" : types, "fields_to_keep" : keepStr}
   return graph.add_operator(OpType.CSV_PARSE, cfg)


def ExtendOperator(graph, typeStr, fldValsList):
  cfg = {"types": typeStr}
  assert len(fldValsList) == len(typeStr) and len(typeStr) < 11 and len(typeStr) > 0
  i = 0
  for x in fldValsList:
    cfg[str(i)] = str(x)
    i += 1
  return graph.add_operator(OpType.EXTEND, cfg)

def TimestampOperator(graph, typeStr):
  cfg = {"type": typeStr}
  assert len(typeStr) < 3 and len(typeStr) > 0
  return graph.add_operator(OpType.TIMESTAMP, cfg)


def GenericParse(graph,
                 pattern, typeStr, field_to_parse = 0, keep_unparsed=True):
    cfg = {"types" : typeStr,
           "pattern" : pattern,
           "field_to_parse" :field_to_parse,
           "keep_unparsed" : str(keep_unparsed)}
    import re
    re.compile(pattern) # throw an error on bad patterns
    return graph.add_operator(OpType.PARSE, cfg)


def RandSource(graph, n, k):
   cfg = {"n": str(n), "k": str(k)}  # "rate":str(rate)
   return graph.add_operator(OpType.RAND_SOURCE, cfg)


def RandEval(graph):
  return graph.add_operator(OpType.RAND_EVAL, {} )

def RandHist(graph):
  return graph.add_operator(OpType.RAND_HIST, {} )


def TRoundOperator(graph, fld, round_to, add_offset=0):
  cfg = {"fld_offset": str(fld),
         "round_to": str(round_to),
         "add_offset": str(add_offset)}  # "rate":str(rate)
  return graph.add_operator(OpType.T_ROUND_OPERATOR, cfg)


def NoOp(graph):
   cfg = {}
   return graph.add_operator(OpType.EXTEND, cfg)

def Ground(graph):
  cfg = {"no_store": "true"}
  return graph.add_operator(OpType.DUMMY_RECEIVER, cfg)

def Project(graph, field):
  return graph.add_operator(OpType.PROJECT, {'field':field})
  

class TimeSubscriber(Operator):
  def __init__ (self, graph, filter, interval, sort_order = "", num_results = 0):
    super(TimeSubscriber,self).__init__(graph,OpType.TIME_SUBSCRIBE, {}, 0)
    self.filter = filter  #maps
    self.cfg["window_size"] = interval
    self.cfg["sort_order"] = sort_order
    self.cfg["num_results"] = num_results

    self.id = graph.add_existing_operator(self)
    assert self.id != 0


  def add_to_PB(self, alter):
    assert( len(self.preds) == 1)
    pred_cube = list(self.preds)[0]
    # Need to convert the user-specified selection keys into positional form
    # for the DB
    dims = pred_cube.get_output_dimensions()
    tuple = Tuple()

#    max_dim = max(dims_by_id.keys())
#   print dims_by_id
#   for id in range(0, max_dim+1):
#      el = tuple.e.add()
#      if id not in dims_by_id:
#        continue
    filter = {}
    filter.update(self.filter)
    for dim_name,dim_type in dims:
      el = tuple.e.add()
      if dim_name in self.filter:
        val = filter[dim_name]
        del filter[dim_name]
        if dim_type == Element.STRING:
          el.s_val = val
        else:
          raise ValueError("Panic; trying to filter on dimension without type")

    if len(filter) > 0:
      unmatched_fields = ",".join(self.filter.keys())
      raise ValueError("Panic: filter field unknown in cube. Unmatched "
                       "fields: {}".format(unmatched_fields))
#    print "final filter tuple:", tuple
    #We do this in two phases

    serialized_filter = tuple.SerializeToString()
#    print "Filter length, serialized: ",len(serialized_filter)
    self.cfg["slice_tuple"] = serialized_filter

    my_meta = Operator.add_to_PB(self,alter)

  def out_schema(self, in_schema):
    if len(in_schema) == 0:
      raise SchemaError("subscriber %s has no inputs"  % self.id)
    check_ts_field(in_schema, self.cfg)
#    print "time subscriber schema"
    return in_schema  #everything is just passed through

def VariableCoarseningSubscriber(*args, **kwargs):
   op = TimeSubscriber(*args, **kwargs)
   op.type = OpType.VAR_TIME_SUBSCRIBE
   return op

def OneShotSubscriber(g, filter, **kwargs):
   op = TimeSubscriber(g, filter, 0, **kwargs)
   op.type = OpType.ONE_SHOT_SUBSCRIBE
   return op

def DelayedSubscriber(g, filter, **kwargs):
   op = TimeSubscriber(g, filter, 0, **kwargs)
   op.type = OpType.DELAYED_SUBSCRIBE
   return op


def LatencyMeasureSubscriber(graph, time_tuple_index, hostname_tuple_index, interval_ms=1000):
   cfg = {"time_tuple_index" : str(time_tuple_index),
          "hostname_tuple_index" : str(hostname_tuple_index),
          "interval_ms" : str(interval_ms)}
   return graph.add_operator(OpType.LATENCY_MEASURE_SUBSCRIBER, cfg)
   
##### Test operators #####

def SendK(graph, k):
   cfg = {"k" : str(k)}
   return graph.add_operator(OpType.SEND_K, cfg)

def RateRecord(graph):
   cfg = {}
   return graph.add_operator(OpType.RATE_RECEIVER, cfg)

def DummySerialize(g):
  return g.add_operator("SerDeOverhead", {})

def Echo(g):
  return g.add_operator(OpType.ECHO, {})

def VariableSampling(g, field=1, type='S'):
  cfg = {'hash_field':field, 'hash_type':type}
  return g.add_operator(OpType.VARIABLE_SAMPLING, cfg)

def SamplingController(g):
  return g.add_operator(OpType.CONGEST_CONTROL, {})

def Quantile(graph, q, field):
   cfg = {"q":str(q), "field":field}
   return graph.add_operator(OpType.QUANTILE, cfg)

def ToSummary(graph, size, field):
   cfg = {"size":str(size), "field":field}
   return graph.add_operator(OpType.TO_SUMMARY, cfg)

def SummaryToCount(graph, field):
   cfg = {"field":field}
   return graph.add_operator(OpType.SUMMARY_TO_COUNT, cfg)

def DegradeSummary(graph, field):
   cfg = {"field":field}
   return graph.add_operator(OpType.DEGRADE_SUMMARY, cfg)

def URLToDomain(graph, field):
   cfg = {"field":field}
   return graph.add_operator(OpType.URLToDomain, cfg)

def TimeWarp(graph, field, warp):
   cfg = {"field":field, "warp":warp}
   return graph.add_operator(OpType.TIMEWARP, cfg)

def CountLogger(graph, field):
   cfg = {"field":field}
   return graph.add_operator(OpType.COUNT_LOGGER, cfg)

def AvgCongestLogger(graph):
   cfg = {}
   return graph.add_operator(OpType.AVG_CONGEST_LOGGER, cfg)

def EqualsFilter(graph, field, targ):
   cfg = {"field":field, "targ":targ}
   return graph.add_operator(OpType.EQUALS_FILTER, cfg)

def GreaterThan(graph, field, targ):
   cfg = {"field":field, "targ":targ}
   return graph.add_operator(OpType.GT_FILTER, cfg)

def RatioFilter(graph, numer, denom, bound):
   cfg = {"numer_field":numer, "denom_field":denom, "bound":bound}
   return graph.add_operator(OpType.RATIO_FILTER, cfg)

def SeqToRatio(graph, url_field, total_field, respcode_field):
    cfg = {"url_field":url_field, "total_field": total_field, "respcode_field":respcode_field}
    return graph.add_operator(OpType.SEQ_TO_RATIO, cfg)


def MultiRoundClient(graph):
   return graph.add_operator(OpType.TPUT_WORKER, {})

def MultiRoundCoord(graph):
   return graph.add_operator(OpType.TPUT_CONTROLLER, {})

def WindowLenFilter(graph):
   return graph.add_operator(OpType.WINDOW_CUTOFF, {})


def FilterSubscriber(graph, cube_field=None, level_in_field=None):
   cfg = {}
   if level_in_field is not None:
    cfg["level_in_field"] = int(level_in_field)
   if level_in_field is not None:
    cfg["cube_field"] = int(cube_field)
   return graph.add_operator(OpType.FILTER_SUBSCRIBER, cfg)
  
def filter_subsc_validate(filter_op, input_schemas):
  saw_cube = False
  saw_filter = False  
  ret = []
  cfg = filter_op.cfg
  
  if len(filter_op.preds) == 0:
      raise SchemaError("subscriber %s has no inputs"  % filter_op.id)  


  for pred in filter_op.preds:

    if isinstance(pred, Cube):
      if saw_cube:
        raise SchemaError("filter should have a cube input and at most one other")
      saw_cube = True
      ret = input_schemas[pred.get_id()]
      print "picking out_schema for filter. Guessing %s" % ret
      #todo check that relevant field is int
    else:
      if saw_filter:
        raise SchemaError("filter should have a cube input and at most one other")
      saw_filter = True
      in_s = input_schemas.get(filter_op.get_id(), [] )


  if len(filter_op.preds) == 1 and not saw_cube:
      raise SchemaError("FilterSubscriber %s has no cube input"  % filter_op.id)  

  if saw_filter and in_s != []:
          
    if not "cube_field" in cfg:
      raise SchemaError("must specify numeric 'cube_field' if adding a filter edge")
    c_in = int(cfg['cube_field'])
    if len(ret) <= c_in:
      raise SchemaError("Can't filter on %d. Schema len is only %d. " \
        " Schema was %s" % (c_in, len(ret), str(ret)))
    if ret[ c_in ][0] != 'I':
      raise SchemaError("schema[c_in=%d] for FilterSubscriber should be int. " \
          "Schema was %s, field %d was %s." % (c_in, str(ret), c_in, str(ret[c_in])))
            
    if not "level_in_field" in cfg:
      raise SchemaError("must specify numeric 'level_in_field' if adding a filter edge")
    level_in = int(cfg['level_in_field'])
    if len(in_s) <= level_in or  in_s[ level_in ][0] != 'I':
      raise SchemaError("schema[level_in_field=%d] for FilterSubscriber should be int. " \
          "Schema was %s." % (level_in, str(in_s)))
  return ret



def BlobReader(graph, dirname, prefix, files_per_window, ms_per_window = 1000):
   cfg = {"dirname":dirname, "prefix":prefix, 'files_per_window':files_per_window, \
              'ms_per_window':ms_per_window}
   return graph.add_operator(OpType.BLOB_READ, cfg)

def IntervalSampling(graph, max_interval):
   return graph.add_operator(OpType.INTERVAL_SAMPLING, {'max_drops':max_interval})
   
def ImageQuality(graph):
   return graph.add_operator(OpType.IMAGE_QUALITY, {})
   
   
   