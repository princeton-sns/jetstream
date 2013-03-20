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

def main():

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


def define_schema_for_raw_cube(cube, ids = [0,1,2,3,4,5,6]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", CubeSchema.Dimension.INT32, ids[1])
  cube.add_dim("url", CubeSchema.Dimension.STRING, ids[2])
  cube.add_agg("size", jsapi.Cube.AggType.COUNT, ids[3])
  cube.add_agg("latency", jsapi.Cube.AggType.COUNT, ids[4])
#  cube.add_agg("count", jsapi.Cube.AggType.COUNT, ids[5])

  cube.set_overwrite(True)


def get_graph(source_nodes, root_node, options):
  ECHO_RESULTS = not options.no_echo
  g= jsapi.QueryGraph()
  BOUND = 100
  
  start_ts = parse_ts(options.start_ts)

  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
     coral_fidxs['URL_requested'], coral_fidxs['nbytes'], coral_fidxs['dl_utime'] ]

  global_results = g.add_cube("global_slow")
  define_schema_for_raw_cube(global_results, parsed_field_offsets)
  global_results.instantiate_on(root_node)

  if ECHO_RESULTS:
    pull_q = jsapi.TimeSubscriber(g, {}, 1000)
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("start_ts", start_ts)
  #    pull_q.set_cfg("rollup_levels", "8,1")
  #    pull_q.set_cfg("simulation_rate",1)
    pull_q.set_cfg("window_offset", 6* 1000) #but trailing by a few
  
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
    g.chain( [global_results, pull_q, echo] )

  for node, i in numbered(source_nodes, False):
  
    f = jsapi.FileRead(g, options.fname, skip_empty=True)
    csvp = jsapi.CSVParse(g, coral_types)
    csvp.set_cfg("discard_off_size", "true")
    round = jsapi.TimeWarp(g, field=1, warp=options.warp_factor)
    round.set_cfg("wait_for_catch_up", "true")
    f.instantiate_on(node)
    
    filter = jsapi.RatioFilter(g, numer=coral_fidxs['dl_utime'], \
      denom = coral_fidxs['nbytes'], bound = BOUND)
    g.chain( [f, csvp, round, filter, global_results] )

  return g

if __name__ == '__main__':
    main()

