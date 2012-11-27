
import unittest

import query_graph as jsapi
from operator_schemas import OpType,SchemaError
from jetstream_types_pb2 import *

class TestSchemas(unittest.TestCase):

  def test_file_and_counter(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    counter = jsapi.RateRecord(qGraph)
    qGraph.connect(reader,counter)
    
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.assertTrue(False, "should not throw, but got " + str(ex))

  def test_bad_edge(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    rounder = jsapi.TRoundOperator(qGraph,2, 2)
    qGraph.connect(reader,rounder)
    
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.assertTrue("can't round field 2" in str(ex))
    else:
      self.assertTrue(False, "should throw, but didn't")


  def test_bad_unify(self):
    qGraph = jsapi.QueryGraph()
    src = jsapi.RandSource(qGraph, 1, 2)
    reader = jsapi.FileRead(qGraph, "file name")
    dest = jsapi.ExtendOperator(qGraph, "s", ["a string"])
    qGraph.connect(reader,dest)
    qGraph.connect(src,dest)

    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.assertTrue( "match existing schema" in str(ex))
#      print "got expected err:", str(ex)
    else:
      self.assertTrue(False, "should throw, but didn't")            
        
  

  def test_cubeInsert(self):

    qGraph = jsapi.QueryGraph()
    local_cube = qGraph.add_cube("results")
    local_cube.add_dim("state", Element.STRING, 0)
    local_cube.add_dim("time", Element.TIME, 1)
    local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
        
    src = jsapi.RandSource(qGraph, 1, 2)
    qGraph.connect(src,local_cube)
  
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.assertTrue(False, "should not throw, but got " + str(ex))


    qGraph.remove(src)
        
      #add a mismatched edge, string versus  string,time
    reader = jsapi.FileRead(qGraph, "file name")
    qGraph.connect(reader,local_cube)


    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      print "got expected err:", str(ex)
    else:
      self.assertTrue(False, "should throw, but didn't")            
        
    


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
    