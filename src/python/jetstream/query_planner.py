import re
import sys
import types

from jsgraph import *
from computation_state import *
from worker_assign import WorkerAssignment

from jetstream_types_pb2 import *


def overwrite_comp_ids (alter, compID):

  alter.computationID = compID
  for operatorMeta in alter.toStart:
    operatorMeta.id.computationID = compID

  for edge in alter.edges:
    edge.computation = compID

  for policy in alter.congest_policies:
    for o in policy.op:
      o.computationID = compID

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

  def __init__ (self, dataplaneEps):
    assert len(dataplaneEps) == 0 or isinstance(dataplaneEps, types.DictType)
    self.workerLocs = dataplaneEps  # map of workerID -> dataplane location [host,port pair]
    return
    
    
  def take_raw_topo (self, altertopo):
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

    validIDs = set({})  # set of operator IDs
    # Organization of this method is parallel to the altertopo structure.
    # First verify top-level metadata. Then operators, then cubes.
    if len(altertopo.toStart) == 0:
      return "Topology includes no operators"
    for op in altertopo.toStart:
      if op.id.task in validIDs:
        return "Operator %d was defined more than once" % (op.id.task)
      validIDs.add(op.id.task)

#  Can't really do this verification -- breaks with UDFs
#    for operator in altertopo.toStart:
#      if not operator.op_typename in KNOWN_OP_TYPES:
#        print "WARNING: unknown operator type KNOWN_OP_TYPES"

    pinnedCubes = {} #true if cube is pinned, false if unpinned      
    for cube in altertopo.toCreate:
      if not self.CUBE_NAME_PAT.match(cube.name):
        return "Invalid cube name %s" % cube.name
      if len(cube.schema.aggregates) == 0:
        return "Cubes must have at least one aggregate per cell"
      if len(cube.schema.dimensions) == 0:
        return "Cubes must have at least one dimension"
      if cube.name in pinnedCubes:
        if cube.name in pinnedCubes and pinnedCubes[cube.name] and cube.HasField("site"):
          print "Cube %s was defined more than once. This is not fatal." % (cube.name)
        else:
          return "Cube %s is defined more than once and not pinned at each" % cube.name
      else:
        pinnedCubes[cube.name] = cube.HasField("site")
      validIDs.add(cube.name)
        
    for edge in altertopo.edges:
      if ((edge.HasField("src") and edge.HasField("src_cube")) or
          (edge.HasField("dest") and edge.HasField("dest_cube"))):
        return "Source/destination can be an operator or cube, not both"
      if edge.HasField("dest_addr"):
        # This is an external edge
        if edge.HasField("dest") or edge.HasField("dest_cube"):
          return "External edge cannot have an operator/cube as destination"
      else:
        srcID = edge.src if edge.HasField("src") else edge.src_cube
        destID = edge.dest if edge.HasField("dest") else edge.dest_cube
        if srcID not in validIDs:
          return "Edge source operator/cube %s has not been defined" % (str(srcID))
        if destID not in validIDs:
          return "Edge destination operator/cube %s has not been defined" % (str(destID))

    return ""

    
  def get_assignments (self, compID):
    """ Creates a set of assignments for this computation.
    Takes the computation ID to use for this computation.
    Returns a map from worker ID to WorkerAssignment
    """

    if not self.alter:
      logger.error("No raw topology specified; need to call take_raw before get_assignments")
      assert(false)

    overwrite_comp_ids(self.alter, compID)
    # Build the computation graph so we can analyze/manipulate it
    jsGraph = JSGraph(self.alter.toStart, self.alter.toCreate, self.alter.edges)
    assignments = {}   # maps worker ID [host,port pair] -> WorkerAssignment
    taskToWorker = {}  # task ID [int or string] -> worker ID
    gSources = jsGraph.get_sources()
    gSinks = jsGraph.get_sinks()
    # Pick a default worker for nodes whose placement is unconstrained by our algorithm
    defaultWorkerID = self.workerLocs.keys()[0]

    # Assign pinned nodes to their specified workers. If a source or sink is unpinned,
    # assign it to a default worker.
    toPin = []
    toPin.extend(self.alter.toStart)
    toPin.extend(self.alter.toCreate)
    multiplyDefinedCubes = set({})
    for gNode in toPin:
      workerID = None
      if gNode.site.address != '':
        # Node is pinned to a specific worker
        workerID = (gNode.site.address, gNode.site.portno)
        # But if the worker doesn't exist, revert to the default worker
        if workerID not in self.workerLocs.keys():
          logger.warning("Node was pinned to nonexistent worker %s" % str(workerID))
          workerID = defaultWorkerID
      elif (gNode in gSources) or (gNode in gSinks):
        # Node is an unpinned source/sink; pin it to a default worker
        workerID = defaultWorkerID
      else:
        # Node will be placed later
        continue
        
      if workerID not in assignments:
        assignments[workerID] = WorkerAssignment(compID)
      assignments[workerID].add_node(gNode)
      
      id = get_oid(gNode)
      if id in multiplyDefinedCubes:
        continue
      if id in taskToWorker:
        del taskToWorker[id]
        multiplyDefinedCubes.add(id)
      else:
        taskToWorker[id] = workerID
    
    # Find the first global union node, aka the LCA of all sources.
    #TODO:SOSP: USE THE SINK'S LOCATION AS THE DEFAULT FOR THE UNION NODE.
    union = jsGraph.get_sources_lca()
    # All nodes from union to sink should be at one site
    unionID = get_oid(union)
    workerID = taskToWorker[unionID] if unionID in taskToWorker else defaultWorkerID
    if workerID not in assignments:
      assignments[workerID] = WorkerAssignment(compID)
    toPlace = jsGraph.get_descendants(union)
    for gNode in toPlace:
      gID = get_oid(gNode)
      if gID not in taskToWorker:
        taskToWorker[gID] = workerID
        assignments[workerID].add_node(gNode)
      else:
        # Enforce stickiness: if a descendant is pinned to a different worker, use the new worker
        # from here on. This works because get_descendants returns nodes in topological order.
        workerID = taskToWorker[gID]
        
    # All nodes from source to union should be at one site, for each source
    for gSource in gSources:
      gID = get_oid(gSource)
      assert(gID in taskToWorker)
      workerID = taskToWorker[gID]
      toPlace = jsGraph.get_descendants(gSource, union)
      for gNode in toPlace:
        gID = get_oid(gNode)
        if gID not in taskToWorker:
          taskToWorker[gID] = workerID
          assignments[workerID].add_node(gNode)
        else:
          # Enforce stickiness: if a descendant is pinned to a different worker, use the new worker
          # from here on. This works because get_descendants returns nodes in topological order.
          workerID = taskToWorker[gID]

    # Assign each edge to the worker where the source was placed, since the source will
    # initiate the connection to the destination
    #TODO: Update for NAT support, which may require dest to contact source
    for edge in self.alter.edges:
      workerID = taskToWorker[edge.src] if edge.HasField("src") else taskToWorker[edge.dest] 
      # If an edge lacks a src, then it's from a cube, and has a dest.
      # Since cube names can be ambiguous, we use the subscriber's location in that case.
      
      if not edge.HasField("dest_addr"):
        if edge.HasField("dest"):
          destWorkerID = taskToWorker[edge.dest]
        else:  #by default, edges to cubes go to the local cube
          assert( edge.HasField("dest_cube"))
          destWorkerID =  taskToWorker.get(edge.dest_cube, workerID)
#         taskToWorker[edge.dest_cube]
        # If the source/dest workers are different, this is a remote edge
        if destWorkerID != workerID:
          # Use the dataplane location of the destination worker
          destLoc = self.workerLocs[destWorkerID]
          edge.dest_addr.address = destLoc[0]
          edge.dest_addr.portno = destLoc[1]
    
      assignments[workerID].add_edge(edge)

    for policy in self.alter.congest_policies:
      worker = taskToWorker[ policy.op[0].task ]
      assignments[worker].add_policy(policy)

    #TODO: WE SHOULD VALIDATE THE ASSIGNMENT HERE

    return assignments
