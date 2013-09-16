import unittest

from controller import *
from worker import *
from computation_state import *
from query_planner import QueryPlanner

from generic_netinterface import JSClient
import query_graph as jsapi
from jetstream_types_pb2 import *


class TestQueryPlanner(unittest.TestCase):

#  def setUp(self):
#    self.controller = Controller(('localhost', 0))


#  def tearDown(self):
#    self.controller.stop()


  def test_bad_topos(self):
    compID = 17

    alter = AlterTopo()
    alter.computationID = compID
    planner = QueryPlanner({})
    
    # No operators
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("no operators" in err)

    # Duplicate operators
    newOp = alter.toStart.add()
    newOp.op_typename = "not a real name"
    newOp.id.computationID = compID
    newOp.id.task = 1
    alter.toStart.extend([newOp])
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("operator" in err) and ("more than once" in err))
    
    # Bad cube name
    del alter.toStart[:]
    alter.toStart.extend([newOp])
    newCube = alter.toCreate.add()
    newCube.name = "name with space"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("invalid cube name" in err)
    
    # Cube without aggregates
    newCube.name = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("aggregate" in err)

    # Cube without dimension
    agg = newCube.schema.aggregates.add()
    agg.name = "count"
    agg.type = "count"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue("dimension" in err)

    # Duplicate cubes
    dim = newCube.schema.dimensions.add()
    dim.name = "count"
    dim.type = Element.INT32
    alter.toCreate.extend([newCube])
    err = planner.validate_raw_topo(alter).lower()
    if len(err) > 0:
      print err    
    self.assertTrue(len(err) > 0)
    self.assertTrue(("cube" in err) and ("more than once" in err))

    # Edge with two sources/dests
    del alter.toCreate[:]
    alter.toCreate.extend([newCube])
    newEdge = alter.edges.add()
    newEdge.src = 1
    newEdge.src_cube = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("source" in err) and ("not both" in err))
    newEdge.ClearField("src")
    newEdge.dest = 1
    newEdge.dest_cube = "valid_name"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("destination" in err) and ("not both" in err))
    newEdge.ClearField("dest_cube")

    # Edge with invalid source/dest
    newEdge.src_cube = "valid_name_2"
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("source" in err) and ("defined" in err))
    newEdge.src_cube = "valid_name"
    newEdge.dest = 2
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("destination" in err) and ("defined" in err))
    newEdge.dest = 1

    # External edge with dest operator/cube
    newEdge.dest_addr.address = "myhost"
    newEdge.dest_addr.portno = 0
    err = planner.validate_raw_topo(alter).lower()
    self.assertTrue(len(err) > 0)
    self.assertTrue(("external" in err) and ("destination" in err))

    # Valid
    newEdge.ClearField("dest_addr")
    err = planner.validate_raw_topo(alter).lower()
    self.assertEquals(len(err), 0) 

    
  def test_1node_plan(self):

    dummyNode = ("host",123)
    planner = QueryPlanner( {dummyNode:dummyNode} )

    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter.add())

    err = planner.take_raw_topo(req.alter[0]).lower()
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummyNode in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummyNode].operators), 1)

  def test_2op_plan(self):
    """This test creates an operator and a cube, attached."""
    dummyNode = ("host",123)
    planner = QueryPlanner( {dummyNode:dummyNode} )

    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
    cube.set_overwrite(True)  #fresh results
  
    qGraph.connect(reader, cube)
    
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter.add())

    err = planner.take_raw_topo(req.alter[0]).lower()
    if len(err) > 0:
      print "Test yielded unexpected error:",err
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummyNode in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummyNode].operators), 1)
    self.assertEquals(len(plan[dummyNode].cubes), 1)
        
    pbToNode = plan[dummyNode].get_pb()
    self.assertEquals(len(pbToNode.alter[0].edges), 1)

  def test_external_edge_plan(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    req = ControlMessage()
    req.type = ControlMessage.ALTER    
    qGraph.add_to_PB(req.alter.add())
    
    MY_PORTNO = 1000
    e = req.alter[0].edges.add()
    e.src = req.alter[0].toStart[0].id.task
    e.computation = 0
    e.dest_addr.address = "myhost"
    e.dest_addr.portno = MY_PORTNO

    dummyNode = ("host",123)
    planner = QueryPlanner( {dummyNode:dummyNode} )
    err = planner.take_raw_topo(req.alter[0]).lower()
    self.assertEquals(len(err), 0)  
    plan = planner.get_assignments(1)
    
    self.assertTrue(dummyNode in plan)
    self.assertEquals(len(plan), 1)
    self.assertEquals(len(plan[dummyNode].operators), 1)
        
    pbToNode = plan[dummyNode].get_pb()
    self.assertEquals(len(pbToNode.alter[0].edges), 1)
    self.assertEquals(pbToNode.alter[0].edges[0].dest_addr.portno, MY_PORTNO)

  def test_with_subscriber(self):
    dummyNode = ("host",123)
    planner = QueryPlanner( {dummyNode:dummyNode} )

    qGraph = jsapi.QueryGraph()
    cube = qGraph.add_cube("local_results")
    cube.add_dim("hostname", Element.STRING, 0)
    cube.add_dim("time", Element.TIME, 1)
    cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
    cube.set_overwrite(True)  #fresh results
 
    subscriber = jsapi.TimeSubscriber(qGraph, {"hostname":"http://foo.com"}, 1000)
    qGraph.connect(cube, subscriber)
    
    err = planner.take_raw_topo(qGraph.get_deploy_pb().alter[0]).lower()
    self.assertEquals(len(err), 0)
    
    plan = planner.get_assignments(1)
    self.assertTrue(dummyNode in plan)
    self.assertEquals(len(plan), 1)
    #print plan[dummyNode].get_pb()


  def test_with_partial_placement(self):
    dummyNode1 = ("host",123)
    dummyNode2 = ("host2",234)

    planner = QueryPlanner( {dummyNode1:dummyNode1, dummyNode2:dummyNode2} )
    g = jsapi.QueryGraph()

    evalOp = jsapi.RandEval(g)

    for node,k in zip([dummyNode1, dummyNode2], range(0,2)):
      src = jsapi.RandSource(g, 1, 2)
      src.set_cfg("rate", 1000)
  
      localCube = g.add_cube("local_results_%d" %k)
      localCube.add_dim("state", Element.STRING, 0)
      localCube.add_dim("time", Element.TIME, 1)
      localCube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
  
      pullOp = jsapi.TimeSubscriber(g, {}, 1000)
      pullOp.set_cfg("ts_field", 1)
      pullOp.set_cfg("window_offset", 1000) #pull every three seconds, trailing by one
      
      extendOp = jsapi.ExtendOperator(g, "s", ["node"+str(k)])
      roundOp = jsapi.TRoundOperator(g, fld=1, round_to=5)
      g.connect(src, localCube)  
      g.connect(localCube, pullOp)
      g.connect(pullOp, extendOp)
      g.connect(extendOp, roundOp)
      g.connect(roundOp, evalOp)

      nID = NodeID()
      nID.address, nID.portno = node
      src.instantiate_on(nID)

    g.validate_schemas()

    err = planner.take_raw_topo(g.get_deploy_pb().alter[0])
    self.assertEquals(len(err), 0)  
    plan = planner.get_assignments(1)
    
    pb1 = plan[dummyNode1].get_pb().alter[0]
    
    subscribers = [x for x in pb1.toStart if "Subscriber" in x.op_typename]
    self.assertEquals(len(subscribers),  len(pb1.toCreate))
    self.assertEquals(len(pb1.toCreate), 1)
    self.assertGreater(len(pb1.toStart), 3)
    self.assertLessEqual(len(pb1.toStart), 4)
    

  def test_line_graph_with_subscriber(self):
    dummyNode1 = ("host", 123)
    dummyNode2 = ("host2", 234)
    dummyNode3 = ("host3", 345)

    planner = QueryPlanner( {dummyNode1:dummyNode1, dummyNode2:dummyNode2, dummyNode3:dummyNode3} )
    g = jsapi.QueryGraph()

    src = jsapi.RandSource(g, 1, 2)
    src.set_cfg("rate", 1000)
  
    localCube = g.add_cube("local_results")
    localCube.add_dim("state", Element.STRING, 0)
    localCube.add_dim("time", Element.TIME, 1)
    localCube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)

    pullOp = jsapi.TimeSubscriber(g, {}, 1000)
    pullOp.set_cfg("ts_field", 1)
    pullOp.set_cfg("window_offset", 1000) #pull every three seconds, trailing by one

    remoteCube = g.add_cube("remote_results")
    remoteCube.add_dim("state", Element.STRING, 0)
    remoteCube.add_dim("time", Element.TIME, 1)
    remoteCube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)

    extendOp = jsapi.ExtendOperator(g, "s", ["node1"])
    roundOp = jsapi.TRoundOperator(g, fld=1, round_to=5)

    # The line graph topology is: src -> cube -> subscriber -> operator(s) -> cube. 
    g.connect(src, localCube)
    g.connect(localCube, pullOp)
    g.connect(pullOp, extendOp)
    g.connect(extendOp, roundOp)
    g.connect(roundOp, remoteCube)

    node1ID = NodeID()
    node1ID.address, node1ID.portno = dummyNode1
    node2ID = NodeID()
    node2ID.address, node2ID.portno = dummyNode2
    node3ID = NodeID()
    node3ID.address, node3ID.portno = dummyNode3

    g.validate_schemas()

    # Pin nothing: everything should be placed on one node
    err = planner.take_raw_topo(g.get_deploy_pb().alter[0])
    self.assertEquals(len(err), 0)  
    plan = planner.get_assignments(1)
    self.assertEquals(len(plan), 1)

    # Pin source (src): everything should be placed on the source node
    src.instantiate_on(node2ID)
    err = planner.take_raw_topo(g.get_deploy_pb().alter[0])
    self.assertEquals(len(err), 0)  
    plan = planner.get_assignments(1)
    self.assertEquals(len(plan), 1)
    self.assertTrue(dummyNode2 in plan)
    
    # Pin source (src) and sink (remoteCube): everything except sink should be on source node
    src.instantiate_on(node2ID)
    remoteCube.instantiate_on(node1ID)
    err = planner.take_raw_topo(g.get_deploy_pb().alter[0])
    self.assertEquals(len(err), 0)
    plan = planner.get_assignments(1)
    self.assertEquals(len(plan), 2)
    node1Plan = plan[dummyNode1]
    node2Plan = plan[dummyNode2]
    self.assertEquals(len(node1Plan.cubes), 1)
    self.assertTrue(node1Plan.cubes[0].name.endswith(remoteCube.name))
    self.assertEquals(len(node1Plan.operators), 0)
    self.assertEquals(len(node2Plan.cubes), 1)
    self.assertTrue(node2Plan.cubes[0].name.endswith(localCube.name))
    self.assertEquals(len(node2Plan.operators), 4)

    # Pin source (src), source cube (localCube), and sink (remoteCube): regardless of where
    # source and sink are placed, source cube up to (but excluding) sink should be on same node
    src.instantiate_on(node2ID)
    localCube.instantiate_on(node3ID)
    remoteCube.instantiate_on(node1ID)
    err = planner.take_raw_topo(g.get_deploy_pb().alter[0])
    self.assertEquals(len(err), 0)
    plan = planner.get_assignments(1)
    self.assertEquals(len(plan), 3)
    node3Plan = plan[dummyNode3]
    self.assertEquals(len(node3Plan.cubes), 1)
    self.assertTrue(node3Plan.cubes[0].name.endswith(localCube.name))
    self.assertEquals(len(node3Plan.operators), 3)
    # In particular, the cube subscriber should be on the same node as the cube!
    pb3 = node3Plan.get_pb().alter[0]
    subscribers = [x for x in pb3.toStart if "Subscriber" in x.op_typename]
    self.assertEquals(len(subscribers), 1)


  def test_serializePolicy(self):

    qGraph = jsapi.QueryGraph()
    local_cube = qGraph.add_cube("results")
    local_cube.add_dim("state", Element.STRING, 0)
    local_cube.add_dim("time", Element.TIME, 1)
    local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
        
    src = jsapi.RandSource(qGraph, 1, 2)
    sub = jsapi.TimeSubscriber(qGraph, {}, 1000, "-count") #pull every second
    sample = jsapi.VariableSampling(qGraph)

    eval_op = jsapi.RandEval(qGraph)
    qGraph.chain( [src, local_cube, sub, sample, eval_op] )
    qGraph.add_policy( [sub,sample] )

    try: 
      pb =  qGraph.get_deploy_pb()
      self.assertEquals( len (pb.alter[0].congest_policies), 1)
      oid = pb.alter[0].congest_policies[0].op[0].task
      self.assertEquals( oid, sub.id)

#      print str(pb.alter)
    except SchemaError as ex:
      self.fail("should not throw, but got " + str(ex))



if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
