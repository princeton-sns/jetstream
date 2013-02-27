import types

from itertools import izip, tee

from jetstream_types_pb2 import *
Dimension = CubeSchema.Dimension
from operator_schemas import SCHEMAS, OpType,SchemaError

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

  def connect(self, oper1, oper2, bwLimit=-1):
    """ Add an edge from the the first operator to the second. """
    aux = {}
    if bwLimit > 0:
      aux['max_kb_per_sec'] = bwLimit
    elif bwLimit == 0:
      aux['dummy'] = True
    
    self.edges[ (oper1.get_id(), oper2.get_id()) ] = aux
    oper2.add_pred(oper1)

  def chain(self, operators):
    """ Add edges from each destination in the list to the next destination in
        the list. Cubes are allowed. """
    assert all(isinstance(op, Destination) for op in operators)
    for oper, next_oper in pairwise(operators):
      self.connect(oper, next_oper)

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
    self.add_to_PB(req.alter)
    return req

  def node_type(self, id):
    if id in self.operators:
      return self.operators[id].type
    else:
      return self.cubes[id].name + " cube"

  def validate_schemas(self):
    worklist = self.get_sources()  #worklist is a list of IDs
#    print "initial source operators",worklist
    input_schema = {}
    for n in worklist:
      input_schema[n] = ()

    forward_edges = self.forward_edge_map();
    for n in worklist:
      # print "validating schemas for outputs of %d" % n
      # note that cubes don't have an output schema and subscribers are a special case
      if n in self.operators:
#        print "found operator",n,"of type",self.operators[n].type
        out_schema = self.operators[n].out_schema( input_schema[n])
#        print "out schema is %s, have %d out-edges" % (str(out_schema), len(forward_edges.get(n, []) ))
      else:
        out_schema = self.cubes[n].out_schema (input_schema[n])

      print "out-schema for", n, self.node_type(n), "is", out_schema

      for o in forward_edges.get(n, []):
        if o in input_schema: #already have a schema:
          if input_schema[o] != out_schema:
            err_msg = "Edge from %d of type %s to %d of type %s (%s) doesn't match existing schema %s" \
                % (n, self.node_type(n), o, self.node_type(o), str(out_schema), str(input_schema[o]))
            raise SchemaError(err_msg)
        else:
          input_schema[o] = out_schema
          worklist.append(o)
        # TODO need to verify the subscribers, and add them to worklist
  #else case is verifying edges out of cubes; different subscribers are different so there's no unique out-schema

# This represents the abstract concept of an operator or cube, for building
# the query graphs. The concrete executable implementations are elsewhere.
class Destination(object):
  def __init__(self, graph, id):
    self.preds = set()  #set of refs, not IDs
    self.graph = graph  #keep link to parent QueryGraph
    self.id = id
    self._location = None


  def add_pred(self, p):
    assert( isinstance(p,Destination) )

    self.preds.add(p)

  def remove_pred(self, src):  #note that argument is a Destination, not an ID
    self.preds.remove(src)

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

  def set_inlink_dummy(self, val=True):
    for p in self.preds:
      self.graph.edges[ (p, self.id) ]['dummy'] = val

  def set_inlink_bwcap(self, val):
    if len(self.preds) == 0:
      raise SchemaError("No predecessors; you need to have some first before setting bwcap")
    for p in self.preds:
      e_attrs = self.graph.edges[ (p.id, self.id) ]
      e_attrs.pop('dummy',None) #clear dummy
      e_attrs['max_kb_per_sec'] = val

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
       d_entry.val = str(val)
     return task_meta

  def set_cfg(self, key, val):
    self.cfg[key] = val

  def out_schema(self, in_schema):
    if self.type in SCHEMAS:
      return SCHEMAS[self.type](in_schema, self.cfg)
    raise SchemaError("Need to define out_schema for %s" % self.type)

  def __str__(self):
    return '({0}, {1})'.format(str(self.type), str(self.cfg))


class Cube(Destination):

  class AggType (object):
    COUNT = "count"
    AVERAGE = "avg"
    STRING = "string"
    MIN_I = "min_i"
    MIN_D = "min_d"
    MIN_T = "min_t"
    HISTO = "quantile_histogram"
    SKETCH = "quantile_sketch"
    SAMPLE = "quantile_sample"


  def __init__(self, graph, name, desc, id):
    super(Cube,self).__init__(graph, id)
    self.name = name
    self.desc = {}
    self.desc.update(desc)
    if 'dims' not in self.desc:
      self.desc['dims'] = []
    if 'aggs' not in self.desc:
      self.desc['aggs'] = []
    self.cached_schema = None

  def add_dim(self, dim_name, dim_type, offset):
    self.desc['dims'].append(  (dim_name, dim_type, offset) )


  def add_agg(self, a_name, a_type, offset):
    self.desc['aggs'].append(  (a_name, a_type, offset) )

  def get_input_dimensions(self):
    """Returns a map from INPUT key to dimension.
    This is NOT the same as the OUTPUT dimensions"""
    r = {}
    for dim_name, dim_type, offset in self.desc['dims']:
      r[offset] = (dim_name, dim_type)
    return r

  def get_output_dimensions(self):
    r = []
    for dim_name, dim_type, offset in self.desc['dims']:
      r.append( (dim_name, dim_type) )
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

  def __str__(self):
    return '({0}, {1})'.format(self.name, self.desc)


  def get_name(self):
    if self._location is not None:
      return  "%s:%d/%s"% (self._location.address, self._location.portno, self.name)
    else:
      return self.name

    # maps from a dimension-type to a typecode. Note that dimensions can't be blobs
  typecode_for_dname = {Dimension.STRING: 'S', Dimension.INT32: 'I',
      Dimension.DOUBLE: 'D', Dimension.TIME: 'T', Dimension.TIME_CONTAINMENT: 'T'} #,  Element.BLOB: 'B' Element.TIME_HIERARCHY: 'H'}

  typecode_for_aname = { 'string':'S', 'count':'I', 'min_i':'I', 'min_d': 'D', 'min_t': 'T',  'blob': 'b', 'quantile_histogram':'Histogram', 'quantile_sketch':'Sketch',
  'quantile_sample':'Sample'}

  def in_schema_map(self):
    """ Returns a map from offset-in-input-tuple to field-type,name pair"""
#    if self.cached_schema is not None:
#      return self.cached_schema
    r = {}
    for name, type, offset in self.desc['dims']:
      r[offset] = (self.typecode_for_dname[type], name)

    for name, type, offset in self.desc['aggs']:
      r[offset] = (self.typecode_for_aname.get(type, "undef:"+type) , name)
    return r


  def out_schema(self, in_schema):
    r = self.in_schema_map()

#    print "in-schema", in_schema
#    print "dims", self.desc['dims']
    max_dim = max([ off for _,_,off in self.desc['dims']])
    if max_dim >= len(in_schema) and len(in_schema) > 0:
      raise SchemaError ("Cube %s has %d dimensions; won't match input %s." % \
          ( self.name, max_dim + 1,str(in_schema)))

#    for (ty,name),i in zip(in_schema, range(0, len(in_schema))):
#      db_schema = r.get(i, ('undef', 'undef'))
#      if ty != db_schema[0]:
    if len(in_schema) > 0:
      for field_id,(ty,name) in r.items():
        if field_id >= len(in_schema):
          print "assuming COUNT for field %s" % name
          continue

        if in_schema[field_id][0] != ty:
          raise SchemaError ("Can't put value %s (type %s) into field %s of type %s" % \
            (in_schema[field_id][1],in_schema[field_id][0], name, ty))
        if in_schema[field_id][1] != name:
          print "Matching input-name %s to cube column %s"  % (in_schema[field_id][1], name)

    ret = []

    for name, type, offset in self.desc['dims']:
      ret.append ( (self.typecode_for_dname[type], name) )

    for name, type, offset in self.desc['aggs']:
      ret.append ( (self.typecode_for_aname.get(type, "undef:"+type) , name)  )
    "cube",self.name,"has schema", ret
    return ret

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
       raise "CSVParse needs either \"all\" or list of field indices to keep,"\
             " got {0}".format(str(fields_to_keep))

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


def TRoundOperator(graph, fld, round_to, add_offset=0):
  cfg = {"fld_offset": str(fld),
         "round_to": str(round_to),
         "add_offset": str(add_offset)}  # "rate":str(rate)
  return graph.add_operator(OpType.T_ROUND_OPERATOR, cfg)


def NoOp(graph):
   cfg = {}
   return graph.add_operator(OpType.EXTEND, cfg)

class TimeSubscriber(Operator):
  def __init__ (self, graph, my_filter, interval, sort_order = "", num_results = 0):
    super(TimeSubscriber,self).__init__(graph,OpType.TIME_SUBSCRIBE, {}, 0)
    self.filter = my_filter  #maps
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
#    print "time subscriber schema"
    if 'ts_field' in self.cfg:
      ts_field = int(self.cfg['ts_field'])
      if ts_field >= len(in_schema):
        raise SchemaError('ts_field %d illegal for operator; only %d real inputs' \
          % (ts_field, len(in_schema)))
      if in_schema[ts_field][0] != 'T':
        raise SchemaError('Expected a time element')
    return in_schema  #everything is just passed through


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

def VariableSampling(g):
  return g.add_operator(OpType.VARIABLE_SAMPLING, {})

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

def URLToDomain(graph, field):
   cfg = {"field":field}
   return graph.add_operator(OpType.URLToDomain, cfg)


def VariableCoarseningSubscriber(*args, **kwargs):
   op = TimeSubscriber(*args, **kwargs)
   op.type = OpType.VAR_TIME_SUBSCRIBE
   return op
