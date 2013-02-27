

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

  g = get_graph(all_nodes, root_node,  options)

  deploy_or_dummy(options, server, g)


def define_cube(cube, ids = [0,1,2,3,4]):
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", Element.INT32, ids[1])
  cube.add_agg("sizes", jsapi.Cube.AggType.HISTO, ids[2])
  cube.add_agg("latencies", jsapi.Cube.AggType.HISTO, ids[3])
  cube.add_agg("count", jsapi.Cube.AggType.COUNT, ids[4])

  cube.set_overwrite(True)


def get_graph(all_nodes, root_node, options):
  g= jsapi.QueryGraph()

  ANALYZE = not options.load_only
  LOADING = not options.analyze_only
  ECHO_RESULTS = not options.noecho

  if not LOADING and not ANALYZE:
    print "can't do neither load nor analysis"
    sys.exit(0)

  start_ts = parse_ts(options.start_ts)

  central_cube = g.add_cube("global_coral_quant")
  central_cube.instantiate_on(root_node)
  define_cube(central_cube)

  if ECHO_RESULTS:
    pull_q = jsapi.TimeSubscriber(g, {}, 1000) #every two seconds
    pull_q.set_cfg("ts_field", 0)
    pull_q.set_cfg("latency_ts_field", 7)
    pull_q.set_cfg("start_ts", start_ts)
    pull_q.set_cfg("rollup_levels", "8,1")
    pull_q.set_cfg("simulation_rate", options.warp_factor)
    pull_q.set_cfg("window_offset", 6* 1000) #but trailing by a few
  
    count_op = jsapi.SummaryToCount(g, 2)
    q_op = jsapi.Quantile(g, 0.95, 3)
    q_op2 = jsapi.Quantile(g, 0.95,2)
    echo = jsapi.Echo(g)
    echo.instantiate_on(root_node)
  
    g.chain([central_cube, pull_q, count_op, q_op, q_op2, echo] )


  latency_measure_op = jsapi.LatencyMeasureSubscriber(g, time_tuple_index=4, hostname_tuple_index=5, interval_ms=100);
      #use field
  echo_op = jsapi.Echo(g);
  echo_op.set_cfg("file_out", options.latencylog)
  echo_op.instantiate_on(root_node)
  g.chain([central_cube, latency_measure_op, echo_op])

  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
      coral_fidxs['nbytes'], coral_fidxs['dl_utime'], len(coral_types) ]

  for node, i in zip(all_nodes, range(0, len(all_nodes))):
    local_cube = g.add_cube("local_coral_quant_%d" %i)
    define_cube(local_cube, parsed_field_offsets)
    print "cube output dimensions:", local_cube.get_output_dimensions()

    if LOADING:
      f = jsapi.FileRead(g, options.fname, skip_empty=True)
      csvp = jsapi.CSVParse(g, coral_types)
      csvp.set_cfg("discard_off_size", "true")
      round = jsapi.TRoundOperator(g, fld=1, round_to=1)
      to_summary1 = jsapi.ToSummary(g, field=parsed_field_offsets[2], size=100)
      to_summary2 = jsapi.ToSummary(g, field=parsed_field_offsets[3], size=100)
      g.chain( [f, csvp, round, to_summary1, to_summary2, local_cube] )
      f.instantiate_on(node)
    else:
       local_cube.set_overwrite(False)
  
    query_rate = 1000 if ANALYZE else 3600 * 1000
    if options.no_backoff:
      pull_from_local = jsapi.TimeSubscriber(g, {}, query_rate)
    else:
      pull_from_local = jsapi.VariableCoarseningSubscriber(g, {}, query_rate)
      
    pull_from_local.instantiate_on(node)
    pull_from_local.set_cfg("simulation_rate", options.warp_factor)
    pull_from_local.set_cfg("ts_field", 0)
    pull_from_local.set_cfg("start_ts", start_ts)
    pull_from_local.set_cfg("window_offset", 2000) #but trailing by a few
#    pull_from_local.set_cfg("rollup_levels", "8,1")
#    pull_from_local.set_cfg("window_size", "5000")

    local_cube.instantiate_on(node)

    timestamp_op= jsapi.TimestampOperator(g, "ms")
    count_extend_op = jsapi.ExtendOperator(g, "i", ["1"])  #why is this here? -asr?
    count_extend_op.instantiate_on(node)      # TODO should get a real hostname here

    timestamp_cube_op= jsapi.TimestampOperator(g, "ms")
    timestamp_cube_op.instantiate_on(root_node)

    g.chain([local_cube, pull_from_local,timestamp_op, count_extend_op, timestamp_cube_op, central_cube])
    if options.bw_cap:
      timestamp_cube_op.set_inlink_bwcap(float(options.bw_cap))
#  g.chain([local_cube, pull_from_local, count_op, q_op, q_op2, echo] )

  return g

if __name__ == '__main__':
    main()

