

from collections import defaultdict
import csv
from optparse import OptionParser
import random
import sys
import time


from jetstream_types_pb2 import *
from remote_controller import *
import query_graph as jsapi
from query_planner import QueryPlanner

from coral_parse import coral_fnames,coral_fidxs, coral_types
from coral_util import *   #find_root_node, standard_option_parser,

desc = """----
This script stores a histogram for all-nodes bandwidth on the root node
----"""
def main():
  print desc

  parser = standard_option_parser()

  (options, args) = parser.parse_args()

  if not options.fname:
    print "you must specify the input file name [with -f]"
    sys.exit(1)

  all_nodes,server = get_all_nodes(options)
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)

  g = get_graph(source_nodes, root_node,  options)

  deploy_or_dummy(options, server, g)


def define_cube(cube, ids = [0,1,2,3,4]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_agg("sizes", jsapi.Cube.AggType.COUNT, ids[1])
  cube.add_dim("hostname", CubeSchema.Dimension.STRING, ids[2])

  cube.set_overwrite(True)


def get_graph(source_nodes, root_node, options):
  g= jsapi.QueryGraph()

  ANALYZE = not options.load_only
  LOADING = not options.analyze_only
  ECHO_RESULTS = not options.no_echo
  ONE_LAYER = True

  if not LOADING and not ANALYZE:
    print "can't do neither load nor analysis"
    sys.exit(0)

  start_ts = parse_ts(options.start_ts)

  central_cube = g.add_cube("global_coral_bw")
  central_cube.instantiate_on(root_node)
  if ONE_LAYER:
    define_cube(central_cube)
  else:
    define_cube(central_cube, [0,2,1])  

  if ECHO_RESULTS:
    pull_q = jsapi.TimeSubscriber(g, {}, 1000) #every two seconds
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("start_ts", start_ts)
#    pull_q.set_cfg("rollup_levels", "8,1")
    pull_q.set_cfg("simulation_rate",1)
    pull_q.set_cfg("window_offset", 4* 1000) #but trailing by a few
  
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
  
    g.chain([central_cube, pull_q, echo] )

  congest_logger = jsapi.AvgCongestLogger(g)
  congest_logger.instantiate_on(root_node)
  g.connect(congest_logger, central_cube)
  
  if not ONE_LAYER:
    n_to_intermediate, intermediates = get_intermediates(source_nodes)
    intermed_cubes = []
    for n, i in zip (intermediates, range(0, len(intermediates))):
      med_cube = g.add_cube("med_coral_bw_%i" % i)
      med_cube.instantiate_on(n)
      med_cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, 0)
      med_cube.add_agg("sizes", jsapi.Cube.AggType.COUNT, 2)
      intermed_cubes.append(med_cube)
      connect_to_root(g, med_cube, n, congest_logger, start_ts)

  for node, i in numbered(source_nodes, not LOADING):
    local_cube = g.add_cube("local_coral_bw_%d" %i)
    local_cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, coral_fidxs['timestamp'])
    local_cube.add_agg("sizes", jsapi.Cube.AggType.COUNT, coral_fidxs['nbytes'])
    
    print "cube output dimensions:", local_cube.get_output_dimensions()

    if LOADING:
      f = jsapi.FileRead(g, options.fname, skip_empty=True)
      csvp = jsapi.CSVParse(g, coral_types)
      csvp.set_cfg("discard_off_size", "true")
      round = jsapi.TimeWarp(g, field=1, warp=options.warp_factor)
      g.chain( [f, csvp, round, local_cube] )
      f.instantiate_on(node)
    else:
       local_cube.set_overwrite(False)

    if ONE_LAYER:
      print node
      my_root = congest_logger      
    else:
      print "multi-layer not yet implemented"
      sys.exit(0)
      intermed_id = n_to_intermediate[node]
      my_root = intermed_cubes[intermed_id]
    connect_to_root(g, local_cube, node, my_root, start_ts, ANALYZE)
  return g


def  get_intermediates(source_nodes):
  # source_nodes is a list of NodeIDs
  prefix = lambda x: x[0:6]
  
  n_to_intermediate = {}
  intermediates = []

  ips = [ (n.address,n) for n in source_nodes]
  ips.sort()
  for (addr, n), id in zip([ips[0], ips[-1] ], [0,1]):
    prefix_mapping [ prefix(addr)] = id
    intermediates.append(n)
  print "prefix map is", prefix_mapping
  for node, i in numbered(source_nodes):
    p = prefix(node.address)
    if p in prefix_mapping:
      n_to_intermediate[i] = prefix_mapping[p]
    else:
      print "no mapping for IP address %s" % n.address
  return n_to_intermediate, intermediates


def connect_to_root(g, local_cube, node, root_op, start_ts, ANALYZE=False):    
      
    query_rate = 1000 if ANALYZE else 3600 * 1000
    pull_from_local = jsapi.TimeSubscriber(g, {}, query_rate)
      
    pull_from_local.instantiate_on(node)
    pull_from_local.set_cfg("simulation_rate", 1)
    pull_from_local.set_cfg("ts_field", 0)
    pull_from_local.set_cfg("start_ts", start_ts)
    pull_from_local.set_cfg("window_offset", 2000) #but trailing by a few
#    pull_from_local.set_cfg("rollup_levels", "8,1")
#    pull_from_local.set_cfg("window_size", "5000")

    local_cube.instantiate_on(node)
    hostname_extend_op = jsapi.ExtendOperator(g, "s", ["${HOSTNAME}"]) 
    hostname_extend_op.instantiate_on(node)

    g.chain([local_cube, pull_from_local, hostname_extend_op, root_op])

if __name__ == '__main__':
    main()

