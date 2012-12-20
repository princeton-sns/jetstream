
import unittest

import query_graph as jsapi
from operator_schemas import *
from jetstream_types_pb2 import *

class TestSchemas(unittest.TestCase):

  def test_file_and_counter(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    counter = jsapi.RateRecord(qGraph)
    qGraph.connect(reader, counter)
    
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.fail("Should not throw, but got: " + str(ex))

  def test_randEval(self):

    qGraph = jsapi.QueryGraph()
    src = jsapi.RandSource(qGraph, 1, 2)
    ex = jsapi.ExtendOperator(qGraph, "i", ["a count"])
    eval = jsapi.RandEval(qGraph)
    qGraph.connect(src,ex)
    qGraph.connect(ex, eval)
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.fail("Should not throw, but got: " + str(ex))

  def test_CSVParse_validate(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    csvprs = jsapi.CSVParse(qGraph, "ISDDDIIDSISIISD")
    qGraph.connect(reader, csvprs)
    try:
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.fail("Should not throw, but got: " + str(ex))

  def test_CVSParse_validate_bad(self):
    qGraph = jsapi.QueryGraph()
    reader = jsapi.FileRead(qGraph, "file name")
    csv_types = "IIIII"
    csvprs = jsapi.CSVParse(qGraph, csv_types)
    # should fail because the outschema of the previous CSVParse has an int
    # as its first element, while CVSParse currently needs a string as the
    # first element. this will probably change when CVSParse supports parsing
    # an arbritrarily indexed tuple, but the validation will be quite similar;
    # probably:
    # assert 'S' != csv_types[3] # note that this is a real assert, not a test
    # csvprs_fail = jsapi.CSVParse(qgraph, csv_types, field_to_parse=3)
    csvprs_fail = jsapi.CSVParse(qGraph, "DDSS")
    qGraph.connect(reader, csvprs)
    qGraph.connect(csvprs, csvprs_fail)
    self.assertRaises(SchemaError, qGraph.validate_schemas)
    # a hack for exceptions with types. also this is new in python 2.7..., so
    # will fail in 2.6 or earlier...
    self.assertRaisesRegexp(SchemaError, '[.\s]*requires a string[.\s]*',
                            qGraph.validate_schemas)

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
      self.fail("should throw, but didn't")


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
      self.fail("should throw, but didn't")
        

  def test_parse_schema(self):
    in_schema = [('I','a number'), ('S', 'to parse') ]
    cfg = {'types':"DSS", 'field_to_parse':1, 'keep_unparsed':str(True)}
    
    out_types = [ ty for ty,_ in validate_parse(in_schema,cfg) ]
    self.assertEquals(out_types, ['I', 'D', 'S', 'S'])

    cfg['keep_unparsed'] = str(False)
    out_types = "".join(t for t,_ in validate_parse(in_schema, cfg))
    self.assertEquals(out_types, cfg['types'])

    cfg['keep_unparsed'] = str(True)
    cfg['field_to_parse'] = 0
    self.assertRaises(SchemaError, validate_parse, in_schema, cfg)


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
      self.fail("should not throw, but got " + str(ex))

    qGraph.remove(src)
        
      #add a mismatched edge, string versus  string,time
    reader = jsapi.FileRead(qGraph, "file name")
    qGraph.connect(reader,local_cube)
    
    e_map = qGraph.forward_edge_map()
    self.assertEquals( len(e_map), 1)
  
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      print "got expected err:", str(ex)
    else:
      self.fail("should throw, but didn't")
      
      
  def test_cubeInsertPartial(self):
    print "inserting tuple with more data than cube"
    #tests insertion where the tuple has more fields than the cube  
    qGraph = jsapi.QueryGraph()
    local_cube = qGraph.add_cube("results")
    local_cube.add_dim("state", Element.STRING, 0)
    src = jsapi.RandSource(qGraph, 1, 2)
    qGraph.connect(src,local_cube)
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.fail("should not throw, but got " + str(ex))

  

        
  def test_cubeSubscribe(self):
  
    qGraph = jsapi.QueryGraph()
    local_cube = qGraph.add_cube("results")
    local_cube.add_dim("state", Element.STRING, 0)
    local_cube.add_dim("time", Element.TIME, 1)
    local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)    

    sub = jsapi.TimeSubscriber(qGraph, {}, 1000, "-count") #pull every second
    eval_op = jsapi.RandEval(qGraph)

    qGraph.connect(local_cube, sub)
    qGraph.connect(sub, eval_op)
    
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.fail("should not throw, but got " + str(ex))
      
    sub2 = jsapi.TimeSubscriber(qGraph, {}, 1000, "-count") #pull every second
    rounder = jsapi.TRoundOperator(qGraph,0, 2)
    qGraph.connect(sub2, rounder)
    qGraph.connect(local_cube, sub2)
  
#    self.assertTrue(1 not in qGraph.operators)
  
    try: 
      qGraph.validate_schemas()
    except SchemaError as ex:
      self.assertTrue("requires that field 0 be a time" in str(ex) )
      print "got expected err:", str(ex)
    else:
      self.fail("should throw, but didn't")
    qGraph.remove(sub2)
    qGraph.remove(rounder)

    


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)

