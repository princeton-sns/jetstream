#
# Graph data structure and algorithms for representing JetStream queries/computations.
#

import logging
from sets import Set

from jetstream_types_pb2 import *

logger = logging.getLogger('JetStream')

def enum(**enums):
      return type('Enum', (), enums)
    
class JSNode(object):
  """Internal representation of a node in a JetStream computation graph. JSNode objects
  should not be returned to an outside caller; return the underlying object instead."""

  # Color enum for graph algorithm
  class Color:
    WHITE = 1
    BLACK = 2
  
  def __init__ (self, id, obj):
    self.id = id
    self.object = obj
    # Declare (and reset) fields used by graph algorithms
    self.reset()

    
  def reset (self):
    self.rank = -1
    self.color = JSNode.Color.WHITE
    self.parent = None
    self.ancestor = None


  def __hash__ (self):
    return hash(self.id) ^ id(self.object)


  def __eq__ (self, other):
    if isinstance(other, JSNode):
      if (self.id != other.id) or (self.object != other.object):
        return False
      return True
    return NotImplemented


  def __ne__ (self, other):
    result = self.__eq__(other)
    if result is NotImplemented:
      return result
    return not result


class JSGraph(object):
  """A JetStream computation graph"""

  def __init__ (self, operators, cubes, edges):
    # Map of node ID -> JSNode
    self.nodes = {}
    # List of source nodes (no incoming edge) and final sink node
    self.sources = []
    self.sink = None
    # Map of node -> [outgoing neighbor]
    self.adjList = {}
    # Map of node -> [incoming neighbor]
    self.radjList = {}

    for op in operators:
      self.nodes[op.id.task] = JSNode(op.id.task, op)
    for c in cubes:
      self.nodes[c.name] = JSNode(c.name, c)

    # Construct the forward and reverse adjacency lists
    for e in edges:
      src = self.nodes[e.src]
      if src not in self.adjList:
        self.adjList[src] = []
      # Determine if the destination is a cube or operator
      dest = self.nodes[e.cube_name] if e.cube_name != "" else self.nodes[e.dest]
      if dest not in self.radjList:
        self.radjList[dest] = []
      self.adjList[src].append(dest)
      self.radjList[dest].append(src)

    # Determine the source nodes and sink node
    for node in self.nodes.values():
      if node not in self.radjList:
        self.sources.append(node)
      if node not in self.adjList:
        # There should only be one sink
        if self.sink != None:
          logger.warn("Multiple sinks found in computation graph")
        self.sink = node
    # There should be at least one source
    if len(self.sources) == 0:
      logger.warn("One or more cycles found in computation graph (were they intended?)")

    # To properly validate a computation graph, use the validate_* methods below
        

  def validate_tree (self):
    #TODO: Ascertain connectivity, tree-like structure (condense parallel paths and cycles)
    return True


  def reset_nodes (self):
    """Clears any node state of prior graph algorithm runs."""
    for node in self.nodes.values():
      node.reset()
      
    
  def get_sources_lca (self):
    """Returns the least-common ancestor (or descendant, more accurately) of the source
    nodes in the graph. Assumes the graph is (reducible to) a tree."""

    # Start with all sources and repeatedly compute the LCA of pairs until one LCA
    # remains. Since LCA(x,x) = x, use a set to track the LCAs.
    lcas = Set(self.sources)
    lcaPairs = []
    while len(lcas) >= 2:
      lcaPairs.append((lcas.pop(), lcas.pop()))
    while len(lcaPairs) > 0:
      # Clear the node state from prior runs
      self.reset_nodes()
      # Compute the LCAs of the pairs, starting at the root (aka sink) of the tree
      self.lca_recurse(self.sink, lcaPairs, lcas)
      # All pairs should be processed and removed by the above call
      assert len(lcaPairs) == 0
      while len(lcas) >= 2:
        lcaPairs.append((lcas.pop(), lcas.pop()))
    assert(len(lcas) == 1)
    return lcas.pop().object


  def lca_recurse (self, u, lcaPairs, lcas):
    uf_make_set(u)
    u.ancestor = u
    # JetStream graphs are directed towards the root, so use the reverse adjacency list.
    if u in self.radjList:
      for v in self.radjList[u]:
        self.lca_recurse(v, lcaPairs, lcas)
        uf_union(u,v)
        uf_find(u).ancestor = u
    u.color = JSNode.Color.BLACK
    # Iterate over a copy of the list so we can delete
    for p in lcaPairs[:]:
      # LCA(u,v) is uf_find(v).ancestor immediately after u is colored black, provided
      # v is already black; otherwise it is uf_find(u).ancestor immediately after v is
      # colored black.
      if (p[0] == u) and (p[1].color == JSNode.Color.BLACK):
        lcas.add(uf_find(p[1]).ancestor)
        lcaPairs.remove(p)
      if (p[1] == u) and (p[0].color == JSNode.Color.BLACK):
        lcas.add(uf_find(p[0]).ancestor)
        lcaPairs.remove(p)


##### Union-find implementation #####

def uf_make_set(x):
  x.parent = x
  x.rank = 0

 
def uf_union(x, y):
  xRoot = uf_find(x)
  yRoot = uf_find(y)
  if xRoot.rank > yRoot.rank:
    yRoot.parent = xRoot
  elif xRoot.rank < yRoot.rank:
    xRoot.parent = yRoot
  elif xRoot != yRoot:
    yRoot.parent = xRoot
    xRoot.rank = xRoot.rank + 1

  
def uf_find(x):
  if x.parent == x:
    return x
  else:
    x.parent = uf_find(x.parent)
    return x.parent
