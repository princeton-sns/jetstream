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
from client_reader import ClientDataReader,tuple_str
from operator_schemas import OpType
from coral_parse import coral_fnames,coral_fidxs, coral_types
from coral_util import *   #find_root_node, standard_option_parser,
import regions

logger = logging.getLogger('JetStream')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

UNION = True
MULTI_LEVEL = False


#  NOTES
#  Raw cube uses source timestamps.
# We then apply a time-warp operator and all other cubes use warped timestamps.

def main():

  parser = standard_option_parser()
  parser.add_option("--mode", dest="mode",
  action="store", help="query to run. Should be 'quantiles' or 'domains'")
  parser.add_option("--wait", dest="wait",
  action="store", help="how long to wait for results")
  (options, args) = parser.parse_args()
  
  if options.mode:
    mode = options.mode
    if len(args) > 0:
      print "Can't specify mode as both an arg and an option."
      sys.exit(0)
  else:
    mode = args[0]
  
  if mode == "quantiles":
    define_internal_cube = quant_cube
    src_to_internal = src_to_quant
    process_results = process_quant
    final_rollup_levels = "8,1"
  elif mode == "domains":
    define_internal_cube = url_cube
    src_to_internal = src_to_url
    process_results = lambda x,y: y
    final_rollup_levels = "8,1,1"
  else:
    print "Unknown mode %s" % mode
    sys.exit(0)

  all_nodes,server = get_all_nodes(options)
  num_nodes = len(all_nodes)
  result_readers = []
  g= jsapi.QueryGraph()

  ops = []
  union_node = find_root_node(options, all_nodes)

# TODO: refactor the below as
#  build_hierarchy(g, define_internal_cube, src_to_internal, options)

  for node in all_nodes:
    if node == union_node and not options.generate_at_union:
      continue
    raw_cube = define_raw_cube(g, "local_records", node, overwrite=False)
    raw_cube_sub = jsapi.TimeSubscriber(g, {}, 1000)
    raw_cube_sub.set_cfg("simulation_rate", options.warp_factor)
    raw_cube_sub.set_cfg("ts_field", 0)
    raw_cube_sub.set_cfg("start_ts", options.start_ts)
    time_shift = jsapi.TimeWarp(g, field=0, warp=options.warp_factor)
    
    last_op = g.chain([raw_cube, raw_cube_sub, time_shift])  #
    last_op = src_to_internal(g, last_op, node, options)
    last_op.instantiate_on(node)
    ops.append(last_op)
    
  if len(ops) == 0:
    print "can't run, no [non-union] nodes"
    sys.exit(0) 
    
  if MULTI_LEVEL:
    r_list = regions.read_regions('regions.txt') 
    cube_in_r = {}
    for (name, defn) in r_list.items():
      node_in_r = regions.get_1_from_region(defn, all_nodes)
      if node_in_r:
        print "for region %s, aggregation is on %s:%d" % (name, node_in_r.address, node_in_r.portno)
        cube_in_r[name] = define_internal_cube(g, "partial_agg", node_in_r)
    for op in ops:
      rgn = regions.get_region(r_list, op.location())
      if not rgn:
        print "No region for node %s:%d" % (op.location().address, op.location().portno)
      g.connect(op, cube_in_r[rgn])

    ops = []
    for cube in cube_in_r.values():
      sub = jsapi.TimeSubscriber(g, filter={})
      sub.set_cfg("window_offset", 2* 1000) #...trailing by a few
#      sub.set_cfg("simulation_rate", options.warp_factor)
#      sub.set_cfg("ts_field", 0)
#      sub.set_cfg("start_ts", options.start_ts)
        #TODO offset
      g.connect(cube, sub)
      ops.append(sub)      
      
  union_cube = define_internal_cube (g, "union_cube", union_node)
  for op in ops:
    g.connect(op, union_cube)
  if options.bw_cap:
    union_cube.set_inlink_bwcap(float(options.bw_cap))

    
  pull_q = jsapi.TimeSubscriber(g, {}, 1000) #only for UI purposes
  pull_q.set_cfg("ts_field", 0)
#  pull_q.set_cfg("latency_ts_field", 7)
#  pull_q.set_cfg("start_ts", start_ts)
  pull_q.set_cfg("rollup_levels", final_rollup_levels)
  pull_q.set_cfg("simulation_rate", 1) #options.warp_factor)
  pull_q.set_cfg("window_offset", 4* 1000) #...trailing by a few

  g.connect(union_cube, pull_q)
  last_op = process_results(g, pull_q)  

  echo = jsapi.Echo(g)
  g.connect(last_op, echo)
    
  deploy_or_dummy(options, server, g)
  


def src_to_quant(g, raw_cube_sub, node, options):
  to_summary1 = jsapi.ToSummary(g, field=2, size=5000)
  to_summary2 = jsapi.ToSummary(g, field=3, size=5000)
  project = jsapi.Project(g, field=2)
  local_cube = quant_cube(g, "summarized_local", node)
  query_rate = 1000
  pull_from_local = jsapi.TimeSubscriber(g, filter={}, interval=query_rate)
  pull_from_local.set_cfg("ts_field", 0)
  pull_from_local.set_cfg("window_offset", 2000)
  
  g.chain( [raw_cube_sub, project, to_summary1, to_summary2, local_cube, pull_from_local] )
  return pull_from_local


def quant_cube(g, cube_name, cube_node):  
  cube = g.add_cube(cube_name)
  cube.instantiate_on(cube_node)
  cube.set_overwrite(True)
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, 0)
  cube.add_dim("response_code", Element.INT32, 1)
  cube.add_agg("sizes", jsapi.Cube.AggType.HISTO, 2)
  cube.add_agg("latencies", jsapi.Cube.AggType.HISTO, 3)
  cube.add_agg("count", jsapi.Cube.AggType.COUNT, 4)
  return cube

def process_quant(g, union_sub):

  count_op = jsapi.SummaryToCount(g, 2)
  q_op = jsapi.Quantile(g, 0.95, 3)
  q_op2 = jsapi.Quantile(g, 0.95,2)

  g.chain([union_sub, count_op, q_op, q_op2] )
  return q_op2


def url_cube(g, cube_name, cube_node):  
  return define_raw_cube(g, "url_intermed", cube_node, overwrite=True)
#  cube = g.add_cube(cube_name)
#  cube.instantiate_on(cube_node)
#  cube.set_overwrite(True)
#  return cube

def src_to_url(g, data_src, node, options):
  raw_cube_sub = data_src.pred_list()[0]
  raw_cube_sub.type = OpType.VAR_TIME_SUBSCRIBE
  raw_cube_sub.set_cfg("max_window_size", options.max_rollup) 
  raw_cube_sub.set_cfg("sort_order", "-count")

#  project = jsapi.Project(g, field=2)
#  g.chain([raw_cube_sub, project])
#  return project
  return data_src

if __name__ == '__main__':
    main()

