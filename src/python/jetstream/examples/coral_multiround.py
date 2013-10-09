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
  all_nodes,server = get_all_nodes(options)
  
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)
  g= jsapi.QueryGraph()

  start_ts = parse_ts(options.start_ts)
  central_cube = define_raw_cube(g, "global_coral_urls", root_node, overwrite=True)

  if not options.no_echo:
    pull_q = jsapi.TimeSubscriber(g, {}, 30000 , sort_order="-count", num_results=10)
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("start_ts", start_ts)
    pull_q.set_cfg("rollup_levels", "6,0,1,0")  # every five seconds to match subscription. Roll up counts.
    pull_q.set_cfg("simulation_rate", 1)
    pull_q.set_cfg("window_offset", 6* 1000) #but trailing by a few
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
    g.chain([central_cube,pull_q, echo] )

  tput_merge = jsapi.MultiRoundCoord(g)
  tput_merge.set_cfg("start_ts", start_ts)
  tput_merge.set_cfg("window_offset", 5 * 1000)
  tput_merge.set_cfg("ts_field", 0)
  tput_merge.set_cfg("num_results", 10)
  tput_merge.set_cfg("sort_column", "-count")
  tput_merge.set_cfg("min_window_size", 5)
  tput_merge.set_cfg("rollup_levels", "10,0,1,0") # roll up response code and referer
  tput_merge.instantiate_on(root_node)
  g.chain ( [tput_merge, central_cube])

  for node in source_nodes:
    local_cube = define_raw_cube(g, "local_records", node, overwrite=False)
#    print "cube output dimensions:", local_cube.get_output_dimensions()
    pull_from_local = jsapi.MultiRoundClient(g)
    pull_from_local.instantiate_on(node)
    lastOp = g.chain([local_cube, pull_from_local, tput_merge])  
    
  deploy_or_dummy(options, server, g)
  
if __name__ == '__main__':
    main()
