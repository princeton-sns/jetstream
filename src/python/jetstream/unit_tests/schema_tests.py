
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
      print "should not throw, but got " + str(ex)
      self.assertTrue(False)

  def test_bad_edge(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    rounder = jsapi.TRoundOperator(qGraph,2, 2)
    qGraph.connect(reader,rounder)
    
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      print str(ex)
    else:
      self.assertTrue(False, "should throw, but didn't")

    
        
    


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
    