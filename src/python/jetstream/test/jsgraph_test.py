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

class TestJSGraph (unittest.TestCase):

  def setUp (self):
    # Create a minimal topology as the basis for our tests: some sources connected to
    # a union operator connected to a sink cube
    self.numSources = 5
    self.operators = [TaskMeta() for i in range(self.numSources + 1)]
    self.cubes = [CubeMeta()]
    self.edges = []
    # The JS graph API only needs computation-local ID information from the nodes
    # (i.e. it assumes all nodes are from the same computation)
    j = 1
    for op in self.operators:
      op.id.task = j
      j += 1
    self.cubes[0].name = "sink_cube"
    # Connect the sources to the union
    for i in range(self.numSources):
      edge = Edge()
      edge.src = self.operators[i].id.task
      edge.dest = self.operators[-1].id.task
      self.edges.append(edge)
    # Connect the union to the sink cube
    edge = Edge()
    edge.src = self.operators[-1].id.task
    edge.cube_name = self.cubes[0].name
    self.edges.append(edge)
    
    
  def tearDown (self):
    pass


  def test_construct (self):
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


  def test_lca_simple (self):
    graph = JSGraph(self.operators, self.cubes, self.edges)
    lca = graph.get_sources_lca()
    # The LCA of the sources should be the union operator
    self.assertEquals(lca, self.operators[-1])


  def test_lca_harder (self):
    # Make the graph more complicated and find the LCA of the sources
    j = self.operators[-1].id.task + 1
    for i in range(4):
      op = TaskMeta()
      op.id.task = j
      self.operators.append(op)
      j += 1
    # Re-wire the graph (sources have ids 1-5, remaining operators are 6-10)
    self.edges = []
    # Connect sources 1 and 2 to op 6
    for i in [1,2]:
      edge = Edge()
      edge.src = i
      edge.dest = 6
      self.edges.append(edge)
    # Connect op 6 to op 7
    edge = Edge()
    edge.src = 6
    edge.dest = 7
    self.edges.append(edge)
    # Connect sources 3 and 4 to op 7
    for i in [3,4]:
      edge = Edge()
      edge.src = i
      edge.dest = 7
      self.edges.append(edge)
    # Connect op 7 to union op 10
    edge = Edge()
    edge.src = 7
    edge.dest = 10
    self.edges.append(edge)
    # Connect source 5 and ops 8, 9, 10 in a linear chain
    chain = [5, 8, 9, 10]
    for i in range(len(chain) - 1):
      edge = Edge()
      edge.src = chain[i]
      edge.dest = chain[i+1]
      self.edges.append(edge)
    # Connect union op 10 to the cube sink
    edge = Edge()
    edge.src = 10
    edge.cube_name = self.cubes[0].name
    self.edges.append(edge)

    graph = JSGraph(self.operators, self.cubes, self.edges)
    lca = graph.get_sources_lca()
    # The LCA of the sources should be the union op 10
    self.assertEquals(lca, self.operators[-1])


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
