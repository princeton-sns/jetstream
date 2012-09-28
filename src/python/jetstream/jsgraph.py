#
# Graph data structure and algorithms for representing JetStream queries/computations.
#

from jetstream_types_pb2 import *

class JSNode(object):

  def __init__(self, id, obj):
    self.id = id
    self.object = obj

    # Declare (and reset) fields used by graph algorithms
    self.reset()
    
  def reset(self):
    self.rank = -1
    self.parent = None


class JSGraph(object):

  def __init__(self, operators, cubes, edges):
    # Map of node ID -> JSNode
    self.nodes = {}
    # List of source nodes (this with no incoming edge)
    self.sources = []
    # Map of node ID -> [node ID of neighbor]
    self.adjList = {}

    for op in operators:
      self.nodes[op.id.task] = JSNode(op.id.task, op)
    for c in cubes:
      self.nodes[c.name] = JSNode(c.name, c)

    nonSources = {}
    # Construct the adjacency list
    for e in edges:
      if e.src not in self.adjList:
        self.adjList[e.src] = []
      if e.cube_name != "":
        # The destination is a cube that cannot be a source
        self.adjList[e.src].append(e.cube_name)
        nonSources[e.cube_name] = None
      else:
        # The destination is an operator that cannot be a source
        self.adjList[e.src].append(e.dest)
        nonSources[e.dest] = None

    # Determine the source nodes
    for node in self.nodes:
      if node.id not in nonSources:
        self.sources.append(node)
        
#   def lca_sources(self):
#      MakeSet(u);
#      u.ancestor := u;
#      for each v in u.children do
#          TarjanOLCA(v);
#          Union(u,v);
#          Find(u).ancestor := u;
#      u.colour := black;
#      for each v such that {u,v} in P do
#          if v.colour == black
#              print "Tarjan's Least Common Ancestor of " + u +
#                    " and " + v + " is " + Find(v).ancestor + ".";


##### Union-find implementation #####

def uf_make_set(x):
  x.parent = x
  x.rank = 0

 
def uf_union(x, y):
  xRoot = find(x)
  yRoot = find(y)
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
