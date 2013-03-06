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
  parser.add_option("--full_url", dest="full_url", action="store_true", default=False)

  (options, args) = parser.parse_args()

  if not options.fname:
    print "you must specify the input file name [with -f]"
    sys.exit(1)

  all_nodes,server = get_all_nodes(options)
  
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)

  g = get_graph(source_nodes, root_node,  options)

  deploy_or_dummy(options, server, g)
  

def define_cube(cube, ids = [0,1,2,3]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", Element.INT32, ids[1])
  cube.add_dim("url", CubeSchema.Dimension.STRING, ids[2])
#  cube.add_agg("sizes", jsapi.Cube.AggType.HISTO, ids[2])
#  cube.add_agg("latencies", jsapi.Cube.AggType.HISTO, ids[3])
  cube.add_agg("count", jsapi.Cube.AggType.COUNT, ids[3])

  cube.set_overwrite(True)


def rollup_for_warp(warp_factor):
#   if warp_factor < 10:
#     return 8
#   elif warp_factor < 50:
#     return 7
#   elif warp_factor < 100:
#     return 6
#   else:
    return 5

def get_graph(source_nodes, root_node, options):
  g= jsapi.QueryGraph()

  ANALYZE = not options.load_only
  LOADING = not options.analyze_only
  ECHO_RESULTS = not options.no_echo


  if not LOADING and not ANALYZE:
    print "can't do neither load nor analysis"
    sys.exit(0)

  start_ts = parse_ts(options.start_ts)

  central_cube = g.add_cube("global_coral_urls")
  central_cube.instantiate_on(root_node)
  define_cube(central_cube)

  if ECHO_RESULTS:
    pull_q = jsapi.TimeSubscriber(g, {}, 5000 , sort_order="-count", num_results=10)
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("start_ts", start_ts)
    pull_q.set_cfg("rollup_levels", "%d,0,1" % rollup_for_warp(options.warp_factor))
    pull_q.set_cfg("simulation_rate", 1)
    pull_q.set_cfg("window_offset", 6* 1000) #but trailing by a few
  
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
  
    g.chain([central_cube,pull_q, echo] )


  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
      coral_fidxs['URL_requested'], len(coral_types) ]

  for node, i in numbered(source_nodes, not LOADING):
    local_cube = g.add_cube("local_coral_urls_%d" %i)
    define_cube(local_cube, parsed_field_offsets)
    print "cube output dimensions:", local_cube.get_output_dimensions()

    if LOADING:
      f = jsapi.FileRead(g, options.fname, skip_empty=True)
      csvp = jsapi.CSVParse(g, coral_types)
      csvp.set_cfg("discard_off_size", "true")
      round = jsapi.TimeWarp(g, field=1, warp=options.warp_factor)
      if not options.full_url:
        url_to_dom = jsapi.URLToDomain(g, field=coral_fidxs['URL_requested'])
        g.chain( [f, csvp, round, url_to_dom, local_cube] )
      else:
        g.chain( [f, csvp, round, local_cube] )
      f.instantiate_on(node)
    else:
       local_cube.set_overwrite(False)

    query_rate = 1000 if ANALYZE else 3600 * 1000
    pull_from_local = jsapi.VariableCoarseningSubscriber(g, {}, query_rate)
    pull_from_local.instantiate_on(node)
    pull_from_local.set_cfg("simulation_rate", 1)
    pull_from_local.set_cfg("ts_field", 0)
    pull_from_local.set_cfg("start_ts", start_ts)
    pull_from_local.set_cfg("max_window_size", 30) #send data at least every 30 seconds

    pull_from_local.set_cfg("window_offset", 2000) #but trailing by a few

    local_cube.instantiate_on(node)
    count_logger = jsapi.CountLogger(g, field=3)

    timestamp_op= jsapi.TimestampOperator(g, "ms")
    count_extend_op = jsapi.ExtendOperator(g, "i", ["1"])
    count_extend_op.instantiate_on(node)

    timestamp_cube_op= jsapi.TimestampOperator(g, "ms")
    timestamp_cube_op.instantiate_on(root_node)
    g.chain([local_cube, pull_from_local,count_logger, timestamp_op, count_extend_op, timestamp_cube_op, central_cube])
    if options.bw_cap:
      timestamp_cube_op.set_inlink_bwcap(float(options.bw_cap))

  return g
  

if __name__ == '__main__':
    main()
