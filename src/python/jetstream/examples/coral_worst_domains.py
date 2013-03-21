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


def define_schema_for_cube(cube, ids = [0,1,2,3,4,5,6]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", CubeSchema.Dimension.INT32, ids[1])
  cube.add_dim("url", CubeSchema.Dimension.STRING, ids[2])
  cube.add_agg("count", jsapi.Cube.AggType.COUNT, ids[3])

  cube.set_overwrite(True)


def get_graph(source_nodes, root_node, options):
  ECHO_RESULTS = not options.no_echo
  ANALYZE = not options.load_only
  LOADING = not options.analyze_only
  
  g= jsapi.QueryGraph()
  
  start_ts = parse_ts(options.start_ts)


  congest_logger = jsapi.AvgCongestLogger(g)
  congest_logger.instantiate_on(root_node)

  global_respcodes = g.add_cube("global_respcodes")
  define_schema_for_cube(global_respcodes)
  global_respcodes.instantiate_on(root_node)

  global_ratios = g.add_cube("global_ratios")
  define_schema_for_cube(global_ratios)
  global_ratios.add_agg("ratio", jsapi.Cube.AggType.MIN_D, 4)
  global_ratios.instantiate_on(root_node)
  
  pull_resp = jsapi.TimeSubscriber(g, {}, 1000)
  pull_resp.set_cfg("ts_field", 0)
  pull_resp.set_cfg("start_ts", start_ts)
  pull_resp.set_cfg("rollup_levels", "8,1,1")
  pull_resp.set_cfg("simulation_rate",1)
  pull_resp.set_cfg("window_offset", 5* 1000)

  compute_ratio = jsapi.SeqToRatio(g, url_field = 2, total_field = 3, respcode_field = 1)

  g.chain( [congest_logger, global_respcodes, pull_resp, compute_ratio, global_ratios] )

  if ECHO_RESULTS:
    pull_q = jsapi.TimeSubscriber(g, {}, 1000, num_results= 5, sort_order="-ratio")
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("start_ts", start_ts)
    pull_q.set_cfg("rollup_levels", "8,1,1")
    pull_q.set_cfg("simulation_rate",1)
    pull_q.set_cfg("window_offset", 12* 1000) #but trailing by a few
  
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
    g.chain( [global_ratios, pull_q, echo] )


  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
     coral_fidxs['URL_requested'],  len(coral_fidxs) ]

  for node, i in numbered(source_nodes, False):

    table_prefix = "local_coral_respcodes";
    table_prefix += "_"+options.warp_factor;
    local_cube = g.add_cube(table_prefix+("_%d" %i))
    define_schema_for_cube(local_cube, parsed_field_offsets)
  
    if LOADING:
      f = jsapi.FileRead(g, options.fname, skip_empty=True)
      csvp = jsapi.CSVParse(g, coral_types)
      csvp.set_cfg("discard_off_size", "true")
      round = jsapi.TimeWarp(g, field=1, warp=options.warp_factor)
      round.set_cfg("wait_for_catch_up", "true")
      f.instantiate_on(node)
      url_to_dom = jsapi.URLToDomain(g, field=coral_fidxs['URL_requested'])
      g.chain( [f, csvp, round, url_to_dom, local_cube ] )   
    else:
       local_cube.set_overwrite(False)
      
    query_rate = 1000 if ANALYZE else 3600 * 1000
    pull_from_local = jsapi.TimeSubscriber(g, {}, query_rate)
      
    pull_from_local.instantiate_on(node)
    pull_from_local.set_cfg("simulation_rate", 1)
    pull_from_local.set_cfg("ts_field", 0)
    pull_from_local.set_cfg("start_ts", start_ts)
    pull_from_local.set_cfg("window_offset", 2000) #but trailing by a few
    local_cube.instantiate_on(node)
    pull_from_local.instantiate_on(node)
    
    g.chain( [local_cube, pull_from_local, congest_logger] )

  return g

if __name__ == '__main__':
    main()

