#
# Graph data structure and algorithms for representing JetStream queries/computations.
#

from jetstream_types_pb2 import *

class JSNode(object):

  def __init__ (self, id, obj):
    self.id = id
    self.object = obj

    # Declare (and reset) fields used by graph algorithms
    self.reset()
    
  def reset (self):
    self.rank = -1
    self.color = 0
    self.parent = None
    self.ancestor = None


class JSGraph(object):

  def __init__ (self, operators, cubes, edges):
    # Map of node ID -> JSNode
    self.nodes = {}
    # List of source node IDs (no incoming edge) and final sink node ID
    self.sources = []
    self.sink = None
    # Map of node ID -> [node ID of outgoing neighbor]
    self.adjList = {}
    # Map of node ID -> [node ID of incoming neighbor]
    self.radjList = {}

    for op in operators:
      self.nodes[op.id.task] = JSNode(op.id.task, op)
    for c in cubes:
      self.nodes[c.name] = JSNode(c.name, c)

    # Construct the forward and reverse adjacency lists
    for e in edges:
      if e.src not in self.adjList:
        self.adjList[e.src] = []
      # Determine if the destination is a cube or operator
      destId = e.cube_name if e.cube_name != "" else e.dest
      if destId not in self.radjList:
        self.radjList[destId] = []
      self.adjList[e.src].append(destId)
      self.radjList[destId].append(e.src)

    #TODO: How do the checks below interact with validation (when does validation happen?)
    
    # Determine the source nodes and sink node
    for node in self.nodes:
      if node not in self.radjList:
        self.sources.append(node)
        print "adding id %d to sources" % (node)
      if node not in self.adjList:
        # There should only be one sink
        print "adding id %d to sink" % (node)
        assert(self.sink == None)
        self.sink = node
    # There should be at least one source
    assert(self.sources)
        

  def validate_tree (self):
    #TODO: Ascertain connectivity, tree-like structure (condense parallel paths and cycles)
    return True


  def lca_sources (self):
    # TODO: make lcas a set
    lcas = []
    lcas.extend(self.sources)
    lcaPairs = []
    while len(lcas) >= 2:
      lcaPairs.append((lcas.pop(), lcas.pop()))
    while len(lcaPairs) >= 1:
      self.lca_recurse(self.sink, lcaPairs, lcas)
      while len(lcas) >= 2:
        lcaPairs.append((lcas.pop(), lcas.pop()))
    assert(len(lcas) == 1)
    return lcas[0]


  def lca_recurse (self, u, lcaPairs, lcas):
    uNode = self.nodes[u]
    uf_make_set(uNode)
    uNode.ancestor = uNode
    # JetStream graphs are directed towards the root, so use the reverse adjacency list.
    if u in self.radjList:
      for v in self.radjList[u]:
        self.lca_recurse(v, lcaPairs, lcas)
        vNode = self.nodes[v]
        uf_union(uNode,vNode)
        uf_find(uNode).ancestor = uNode
    uNode.color = 1
    # Iterate over a copy of the list so we can delete
    for p in lcaPairs[:]:
      vNode = self.nodes[p[1]]
      if (p[0] == u) and (vNode.color == 1):
        lca = uf_find(vNode).ancestor
        lcas.append(lca.id)
        print "LCA of %d and %d is %d" % (p[0], p[1], lca.id)
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
