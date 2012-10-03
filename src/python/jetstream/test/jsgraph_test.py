#
# Tests the JetStream graph interface in jsgraph.py. The graphs created here consist
# of minimally defined operators and cubes. They should not be used as real queries
# submitted to a controller.
#

import random
import os
import signal
import subprocess
import thread
import time
import unittest

from jsgraph import *
from jetstream_types_pb2 import *

class TestJSGraph(unittest.TestCase):

  def setUp(self):
    # Create a minimal topology as the basis for our tests: some sources connected to
    # a union operator connected to a sink cube
    self.numSources = 5
    self.compId = 1
    self.operators = [TaskMeta() for i in range(self.numSources + 1)]
    self.cubes = [CubeMeta()]
    self.edges = []
    # The JS graph API only needs ID information from the nodes
    i = 1
    for op in self.operators:
      op.id.computationID = self.compId
      op.id.task = i
      i += 1
    self.cubes[0].name = "sink_cube"
    # Connect the sources to the union
    for i in range(self.numSources):
      newEdge = Edge()
      newEdge.computation = self.compId
      newEdge.src = self.operators[i].id.task
      newEdge.dest = self.operators[-1].id.task
      self.edges.append(newEdge)
    # Connect the union to the sink cube
    newEdge = Edge()
    newEdge.computation = self.compId
    newEdge.src = self.operators[-1].id.task
    newEdge.cube_name = self.cubes[0].name
    self.edges.append(newEdge)
    
    
  def tearDown(self):
    pass


  def test_construct(self):
    graph = JSGraph(self.operators, self.cubes, self.edges)
    # Create JSNode objects to ease comparisons
    jsnSink = JSNode(self.cubes[0].name, self.cubes[0])
    jsnUnion = JSNode(self.operators[-1].id.task, self.operators[-1])
    # Test the sink cube
    self.assertEquals(graph.sink, jsnSink)
    self.assertTrue(jsnSink in graph.radjList)
    self.assertEquals(len(graph.radjList[jsnSink]), 1)
    self.assertEquals(graph.radjList[jsnSink][0], jsnUnion)
    # Test the union operator
    self.assertTrue(jsnUnion in graph.adjList)
    self.assertEquals(len(graph.adjList[jsnUnion]), 1)
    self.assertEquals(graph.adjList[jsnUnion][0], jsnSink)
    self.assertTrue(jsnUnion in graph.radjList)
    self.assertEquals(len(graph.radjList[jsnUnion]), self.numSources)
    # Test the sources
    for i in range(self.numSources):
      jsnSource = JSNode(self.operators[i].id.task, self.operators[i])
      self.assertTrue(jsnSource in graph.sources)
      self.assertTrue(jsnSource in graph.adjList)
      self.assertEquals(len(graph.adjList[jsnSource]), 1)
      self.assertEquals(graph.adjList[jsnSource][0], jsnUnion)
      self.assertTrue(jsnSource in graph.radjList[jsnUnion])


  def test_lca_sources(self):
    graph = JSGraph(self.operators, self.cubes, self.edges)
    lca = graph.lca_sources()
    # The LCA of the sources should be the union operator
    self.assertEquals(lca, self.operators[-1])

    # Create a more complicated graph and find the LCA
    

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
