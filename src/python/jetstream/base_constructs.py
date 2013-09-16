import types

from jetstream_types_pb2 import *
Dimension = CubeSchema.Dimension
from operator_schemas import SCHEMAS, OpType,SchemaError


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
    
  def pred_list(self):
    return [x for x in self.preds]

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
    c_meta.name = self.qualified_name()
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

  def qualified_name(self):
    return "%d/%s" % (self.id, self.name)
    
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
    messages = []

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
          messages.append("assuming COUNT for field %s" % name)
          continue

        if in_schema[field_id][0] != ty:
          raise SchemaError ("Can't put value %s (type %s) into field %s of type %s" % \
            (in_schema[field_id][1],in_schema[field_id][0], name, ty))
        if in_schema[field_id][1] != name:
          messages.append("Matching input-name %s to cube column %s"  % \
                     (in_schema[field_id][1], name))

    ret = []

    for name, type, offset in self.desc['dims']:
      ret.append ( (self.typecode_for_dname[type], name) )

    for name, type, offset in self.desc['aggs']:
      ret.append ( (self.typecode_for_aname.get(type, "undef:"+type) , name)  )
    "cube",self.name,"has schema", ret
    return ret
