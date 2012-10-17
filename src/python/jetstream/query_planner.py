import re
import sys
import types

from jsgraph import *
from computation_state import *
from worker_assign import WorkerAssignment

from jetstream_types_pb2 import *

class QueryPlanner (object):
  """Stages of computation compilation:

  There's the raw AlterTopo from the client.  Call this the original plan.

  There's turning multi-node location specifiers (e.g. "on all nodes") into actual
  node IDs. The client can do this.  Call the result an expanded plan.

  This plan can be optimized. This involves replacing groups of operators with
  equivalent groups, pushing operators back (towards sources) for partial aggregation,
  converting logical operators into physical operators (sometimes adding operators),
  and others.  Call the result an optmized plan.

  There's placement.  Call the result a placed plan.  This can still include logical
  operators that don't map exactly to physical operators.

  Last, there's putting in assorted plumbing.  Call the result a concrete computation
  plan.  This must only have concrete physical operators. It will be cut up and sent
  to the relevant dataplane nodes for execution according to above placement.

  Validation can happen at several points. Typechecking and suchlike can happen on the
  expanded plan.
  """  

  def __init__ (self, aliveWorkers):
    assert len(aliveWorkers)==0 or \
      isinstance(aliveWorkers, types.DictType)  #should be a list of host/port pairs
    self.workers = aliveWorkers  
    return
    
    
  def take_raw (self, altertopo):
    """Takes the client's plan, validates, and fills in host names.
     For now, we only allow real host names from the client, so this is purely a validation phase.
     Returns a string on error.
     """
     
    err = self.validate_raw_topo(altertopo)
    if len(err) > 0:
      return err
    self.alter = altertopo  
    return ""
  
  
  CUBE_NAME_PAT = re.compile("[a-zA-Z0-9_]+$")
  def validate_raw_topo (self,altertopo):
    """Validates a topology. Should return an empty string if valid, else an error message."""
    
    # Organization of this method is parallel to the altertopo structure.
    # First verify top-level metadata. Then operators, then cubes.
    if len(altertopo.toStart) == 0:
      return "Topology includes no operators"

#  Can't really do this verification -- breaks with UDFs
#    for operator in altertopo.toStart:
#      if not operator.op_typename in KNOWN_OP_TYPES:
#        print "WARNING: unknown operator type KNOWN_OP_TYPES"
      
    for cube in altertopo.toCreate:
      if not self.CUBE_NAME_PAT.match(cube.name):
        return "invalid cube name %s" % cube.name
      if len(cube.schema.aggregates) == 0:
        return "cubes must have at least one aggregate per cell"
      if len(cube.schema.dimensions) == 0:
        return "cubes must have at least one dimension"

    # TODO check that both endpoints of each edge are defined by the computation

    return ""


  def overwrite_operator_comp_ids (self, compID):
    for operatorMeta in self.alter.toStart:
      operatorMeta.id.computationID = compID

    for edge in self.alter.edges:
      edge.computation = compID

    
  def get_assignments (self, compID):
    """ Creates a set of assignments for this computation.
    Takes the computation ID and a list of worker addresses (as host,port pairs).
    Returns a map from worker address to WorkerAssignment
    """
    if not self.alter:
      print "Need to call take_raw before get_assignments    "
      sys.exit(0)

    altertopo = self.alter
    self.overwrite_operator_comp_ids(compID)
    # Build the computation graph so we can analyze/manipulate it
    jsGraph = JSGraph(altertopo.toStart, altertopo.toCreate, altertopo.edges)
    # Set up the computation
    
    assignments = {}  # maps node ID [host/port pair] to a WorkerAssignment
    taskLocations = {}  # task ID [int or string] to host/port pair
    sources = jsGraph.get_sources()
    sink = jsGraph.get_sink()
    defaultEndpoint = self.workers.keys()[0]

    # Assign pinned nodes to their specified workers. If a source or sink is unpinned,
    # assign it to a default worker.
    toPin = []
    toPin.extend(altertopo.toStart)
    toPin.extend(altertopo.toCreate)
    for graph_node in toPin:
      endpoint = None
      if graph_node.site.address != '':
        # Node is pinned to a specific worker
        endpoint = (graph_node.site.address, graph_node.site.portno)
        # But if the worker doesn't exist, revert to the default worker
        if endpoint not in self.workers.keys():
          logger.warning("Node was pinned to a worker, but that worker does not exist")
          endpoint = defaultEndpoint
      elif (graph_node in sources) or (graph_node == sink):
        # Node is an unpinned source/sink; pin it to a default worker
        endpoint = defaultEndpoint
      else:
        # Node will be placed later
        continue
        
      if endpoint not in assignments:
        assignments[endpoint] = WorkerAssignment(compID)
      
      assignments[endpoint].add_node(graph_node)
      #TODO: Sid will consolidate with Computation data structure
      nodeId = graph_node.id.task if isinstance(graph_node, TaskMeta) else graph_node.name
      taskLocations[nodeId] = endpoint
    
    # Find the first global union node, aka the LCA of all sources.
    union = jsGraph.get_sources_lca()
    # All nodes from union to sink should be at one site
    nodeId = union.id.task if isinstance(union, TaskMeta) else union.name
    endpoint = taskLocations[nodeId] if nodeId in taskLocations else defaultEndpoint
    if endpoint not in assignments:
      assignments[endpoint] = WorkerAssignment(compID)
    toPlace = jsGraph.get_descendants(union)
    for node in toPlace:
      nodeId = node.id.task if isinstance(node, TaskMeta) else node.name
      if nodeId not in taskLocations:
        assignments[endpoint].add_node(node)
        taskLocations[nodeId] = endpoint
    # All nodes from source to union should be at one site, for each source
    for source in sources:
      nodeId = source.id.task if isinstance(source, TaskMeta) else source.name
      assert(nodeId in taskLocations)
      endpoint = taskLocations[nodeId]
      toPlace = jsGraph.get_descendants(source, union)
      for node in toPlace:
        nodeId = node.id.task if isinstance(node, TaskMeta) else node.name
        if nodeId not in taskLocations:
          assignments[endpoint].add_node(node)
          taskLocations[nodeId] = endpoint
    
    for edge in altertopo.edges:
      src_host = taskLocations[edge.src]
      if edge.dest or edge.cube_name:
        destID = edge.dest if edge.HasField("dest") else str(edge.cube_name)
        dest_host = taskLocations[destID]
      # If the unique source/dest ids of the edge are different, this is a remote edge
        if dest_host != src_host:
          # Use the dataplane endpoint of the destination
          dest_ep = self.workers[dest_host]
          edge.dest_addr.address = dest_ep[0]
          edge.dest_addr.portno = dest_ep[1]
    
      assignments[src_host].add_edge(edge)

    return assignments    
        
