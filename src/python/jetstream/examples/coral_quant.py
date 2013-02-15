

from collections import defaultdict
import csv
from optparse import OptionParser 
import random
import sys
import time

from jetstream_types_pb2 import *
from remote_controller import *
import query_graph as jsapi

from coral_parse import coral_fnames,coral_fidxs, coral_types

def main():

  parser = OptionParser()

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")
  parser.add_option("-d", "--dry-run", dest="DRY_RUN", action="store_true", 
                  help="shows PB without running", default=False)
  parser.add_option("-r", "--rate", dest="rate",help="the rate to use per source (instead of rate schedule)")
  parser.add_option("-u", "--union_root_node", dest="root_node",help="address of union/aggregator node")
  parser.add_option("-f", "--file_name", dest="fname",help="name of input file")

  parser.add_option("-g", "--generate-at-union", dest="generate_at_union", action="store_false",help="generate data at union node", default=True)


  (options, args) = parser.parse_args()

  if not options.fname:
    print "you must specify the input file name [with -f]"
    sys.exit(1)

  if options.DRY_RUN:
    id = NodeID()
    id.address ="somehost"
    id.portno = 12345
    all_nodes = [id]
  else:    
    server = RemoteController()
    server.connect(serv_addr, serv_port)
    all_nodes = server.all_nodes()


  serv_addr, serv_port = normalize_controller_addr(options.controller)

  root_node = find_root_node(options, all_nodes)

  g = get_graph(all_nodes, root_node, options.fname)

  req = g.get_deploy_pb()
  if options.DRY_RUN:
    print req
  else:
   server.deploy_pb(req)
    

def find_root_node(options, all_nodes):
  if options.root_node:
    found = False
    for node in all_nodes:
      if node.address == options.root_node:
        root_node = node
        found = True 
        break
    if not found:
      print "Node with address: ",options.root_node," not found for use as the aggregator node"
      sys.exit()
  else:
    root_node = all_nodes[0]  #TODO randomize
  return root_node


def define_cube(cube, ids = [0,1,2,3]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", Element.INT32, ids[1])
  cube.add_agg("sizes", jsapi.Cube.AggType.HISTO, ids[2])
  cube.add_agg("latencies", jsapi.Cube.AggType.HISTO, ids[3])

def get_graph(all_nodes, root_node, file_to_parse):
  g= jsapi.QueryGraph()

  central_cube = g.add_cube("global_results")
  define_cube(central_cube)
  pull_q = jsapi.TimeSubscriber(g, {}, 2000) #every two seconds
  q_op = jsapi.Quantile(g, 0.95, 3)
  q_op2 = jsapi.Quantile(g, 0.95,2)
  echo = jsapi.Echo(g)
  
  
  g.chain([central_cube, pull_q, q_op, q_op2, echo] )

  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
      coral_fidxs['nbytes'], coral_fidxs['dl_utime'] ]
      
  for node in all_nodes:
    local_cube = g.add_cube("local_results-" + node.address)
    define_cube(local_cube, parsed_field_offsets)

    f = jsapi.FileRead(g, file_to_parse, skip_empty=True)
    csvp = jsapi.CSVParse(g, coral_types)
    round = jsapi.TRoundOperator(g, fld=1, round_to=1)
    to_summary1 = jsapi.ToSummary(g, field=parsed_field_offsets[2], size=100)
    to_summary2 = jsapi.ToSummary(g, field=parsed_field_offsets[3], size=100)
    g.chain( [f, csvp, round, to_summary1, to_summary2, local_cube] )
    
    pull_from = jsapi.TimeSubscriber(g, {}, 2000) #every two seconds
    g.chain([local_cube, pull_from, central_cube])

    
  
  return g

if __name__ == '__main__':
    main()
    