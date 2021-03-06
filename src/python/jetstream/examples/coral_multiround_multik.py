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
  parser.add_option("-k", dest="k",
  action="store", help="number of results")
  (options, args) = parser.parse_args()
  all_nodes,server = get_all_nodes(options)
  
  if options.k:
    kseq = [int(options.k)]
  else:
    kseq = [2, 10, 20, 30, 40, 50, 75, 100, 150, 200]
  for k in kseq:
    print "\n---------- Trying with k = %d -----------" % k
    generate_and_run(options, all_nodes, server, k)
    if len(kseq) > 1:
      time.sleep(180)  #no immediate feedback on execution so we need to do it this way.
      #yuck
  

def generate_and_run(options, all_nodes, server, k):
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)
  g= jsapi.QueryGraph()

  start_ts = parse_ts(options.start_ts)
  central_cube = define_raw_cube(g, "global_coral_urls", root_node, overwrite=True)

  if not options.no_echo:
    pull_q = jsapi.DelayedSubscriber(g, {}, sort_order="-count", num_results=k)
#    pull_q.set_cfg("ts_field", 0)
#    pull_q.set_cfg("start_ts", start_ts)
    pull_q.set_cfg("rollup_levels", "0,0,1")  
    pull_q.set_cfg("window_offset", 20* 1000) #but trailing by a few
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
    g.chain([central_cube,pull_q, echo] )

  tput_merge = jsapi.MultiRoundCoord(g)
#  tput_merge.set_cfg("start_ts", start_ts)
#  tput_merge.set_cfg("window_offset", 5 * 1000)
#  tput_merge.set_cfg("ts_field", 0)
  tput_merge.set_cfg("wait_for_start", 10)
  tput_merge.set_cfg("num_results", k)
  tput_merge.set_cfg("sort_column", "-count")
#  tput_merge.set_cfg("min_window_size", 5)
  tput_merge.set_cfg("rollup_levels", "0,0,1") # roll up time, response code and referer
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
