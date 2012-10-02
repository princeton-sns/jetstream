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
    # a union operator connected to a sink
    self.numSources = 5
    self.compId = 1
    self.operators = [TaskMeta() for i in range(self.numSources + 2)]
    #self.cubes = [CubeMeta()]
    self.edges = []
    # The JS graph API only needs ID information from the nodes
    i = 1
    for op in self.operators:
      op.id.computationID = self.compId
      op.id.task = i
      i += 1
    #self.cubes[0].name = "unite_sources"
    # Connect the sources to the union
    for i in range(self.numSources):
      newEdge = Edge()
      newEdge.computation = self.compId
      newEdge.src = self.operators[i].id.task
      #newEdge.cube_name = self.cubes[0].name
      newEdge.dest = self.operators[-2].id.task
      self.edges.append(newEdge)
    # Connect the union to the sink
    newEdge = Edge()
    newEdge.computation = self.compId
    #newEdge.src = self.cubes[0].name
    newEdge.src = self.operators[-2].id.task
    newEdge.dest = self.operators[-1].id.task
    self.edges.append(newEdge)
    
    
  def tearDown(self):
    pass


  def test_construct(self):
    graph = JSGraph(self.operators, [], self.edges)
    # Test sink operator
    self.assertEquals(graph.sink, self.operators[-1].id.task)
    self.assertTrue(self.operators[-1].id.task in graph.radjList)
    self.assertEquals(len(graph.radjList[self.operators[-1].id.task]), 1)
    self.assertEquals(graph.radjList[self.operators[-1].id.task][0], self.operators[-2].id.task)
    # Test union operator
    self.assertTrue(self.operators[-2].id.task in graph.adjList)
    self.assertEquals(len(graph.adjList[self.operators[-2].id.task]), 1)
    self.assertEquals(graph.adjList[self.operators[-2].id.task][0], self.operators[-1].id.task)
    self.assertTrue(self.operators[-2].id.task in graph.radjList)
    self.assertEquals(len(graph.radjList[self.operators[-2].id.task]), self.numSources)
#    self.assertEquals(graph.radjList[self.operators[-1].id.task][0], self.cubes[0].name)
#    self.assertTrue(self.cubes[0].name in graph.adjList)
#    self.assertEquals(len(graph.adjList[self.cubes[0].name]), 1)
#    self.assertEquals(graph.adjList[self.cubes[0].name][0], self.operators[-1].id.task)
#    self.assertTrue(self.cubes[0].name in graph.radjList)
#    self.assertEquals(len(graph.radjList[self.cubes[0].name]), self.numSources)
    for i in range(self.numSources):
      self.assertTrue(self.operators[i].id.task in graph.sources)
      self.assertTrue(self.operators[i].id.task in graph.adjList)
      self.assertEquals(len(graph.adjList[self.operators[i].id.task]), 1)
      self.assertEquals(graph.adjList[self.operators[i].id.task][0], self.operators[-2].id.task)
      self.assertTrue(self.operators[i].id.task in graph.radjList[self.operators[-2].id.task])

    # TODO: validate should pass


  def test_lca_sources(self):
    graph = JSGraph(self.operators, [], self.edges)
    lca = graph.lca_sources()
    # The LCA should be the union operator
    self.assertEquals(lca, self.operators[-2].id.task)

if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
