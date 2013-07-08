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
MULTI_LEVEL = True

def main():

  parser = standard_option_parser()
  parser.add_option("--mode", dest="mode",
  action="store", help="query to run. Should be 'trivial' or 'counts'")
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
  
  if mode == "trivial":
    make_graph = make_trivial_graph
    display_results = lambda x: x
  elif mode == "counts":
    print "returning total counts by response code"
    make_graph = make_counts_query_graph
    display_results = display_respcode_results
  elif mode == "by_time":
    print "computing counts by time"  
    make_graph = make_time_query_graph
    display_results = display_time_results
  elif mode == "all":
    make_graph = get_simple_qgraph
    display_results = display_all_results
  else:
    print "Unknown mode %s" % options.mode
    sys.exit(0)

  all_nodes,server = get_all_nodes(options)
  num_nodes = len(all_nodes)
  result_readers = []
  g= jsapi.QueryGraph()

  ops = []
  union_node = find_root_node(options, all_nodes)

  for node in all_nodes:
    if node == union_node and not options.generate_at_union:
      continue
    last_op = make_graph(g, node)
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
        cube_in_r[name] = define_raw_cube(g, "partial_agg", node_in_r, overwrite=True)
    for op in ops:
      rgn = regions.get_region(r_list, op.location())
      if not rgn:
        print "No region for node %s:%d" % (op.location().address, op.location().portno)
      g.connect(op, cube_in_r[rgn])

    ops = []
    for cube in cube_in_r.values():
      sub = jsapi.DelayedSubscriber(g, filter={})
      g.connect(cube, sub)
      ops.append(sub)      
      

  if UNION:
    cube = define_raw_cube (g, "union_cube", union_node, overwrite=True)
    for op in ops:
      g.connect(op, cube)
    sub = jsapi.DelayedSubscriber(g, filter={})
#    sub.instantiate_on(union_node)  #hopefully mooted by new placement code
    g.connect(cube, sub)
    ops = [sub]
    
  print "total of %d ops" % len(ops)
  for last_op in ops:  
    reader = ClientDataReader()
    g.connectExternal(last_op, reader.prep_to_receive_data())
    result_readers.append(reader)

  deploy_or_dummy(options, server, g)
  
  completed = 0
  t = 0
  MS_PER_TICK = 20
  TIME_TO_WAIT = 20  # seconds
  if options.wait:
    TIME_TO_WAIT = int (options.wait)
  TICKS_TO_WAIT = 1000 * TIME_TO_WAIT / MS_PER_TICK
  start_time = time.time()
  while t < TICKS_TO_WAIT and completed < len(ops):
    time.sleep( MS_PER_TICK / 1000.0 )
    t += 1
    completed = sum( [r.is_finished for r in result_readers])
    tuples = sum( [r.tuples_received for r in result_readers])
    if t % 10 == 0:
      logger.info("tick; %d readers completed. %d total tuples." % (completed, tuples))

  duration = time.time() - start_time
  logger.info("finished. %d of %d readers completed. %d total tuples. Total time taken was %d ms" % (completed, len(ops), tuples, duration * 1000))
  if completed == 0:
    sys.exit(0)
  display_results([r for r in result_readers if r.is_finished])

def make_trivial_graph(g, node):
     
  echoer = jsapi.SendK(g, 40)
  return echoer


def get_simple_qgraph(g, node):
  cube = define_raw_cube(g, "local_records", node, overwrite=False)
  
  pull_from_local = jsapi.OneShotSubscriber(g, filter={})
#  pull_from_local.set_cfg("sort_order", "time")  #just a performance experiment
  pull_from_local.instantiate_on(node) #needed since cube names are ambiguous
  g.connect(cube, pull_from_local)
  return pull_from_local
  

def make_counts_query_graph(g, node):
  pull_from_local = get_simple_qgraph(g,node)
  pull_from_local.set_cfg("rollup_levels", "0,1,0")  #roll up everything but response code
  return pull_from_local


def  display_respcode_results(result_readers):
  code_to_count = defaultdict(int)
  did_a_tuple = False
  for reader in result_readers:
    for t in reader:
      if not did_a_tuple:
        did_a_tuple=True
        print tuple_str(t)
      code = t.e[1].i_val
      val = t.e[5].i_val
      code_to_count[code] += val
  
  for k,v in sorted(code_to_count.items()):
    print "Code %d:  %d" % (k, v)


def make_time_query_graph(g, node):
  pull_from_local = get_simple_qgraph(g,node)
  pull_from_local.set_cfg("rollup_levels", "8,0,0")  #roll up everything but response code
        # Rollup level 8 = every-five-seconds; 9 = by second
  return pull_from_local

def  display_time_results(result_readers):
  time_to_count = defaultdict(int)

  for reader in result_readers:
    for req in reader:
      time = req.e[0].t_val
      val = req.e[5].i_val
      time_to_count[time] += val
  print "%d unique time values; %d requests" % (len(time_to_count),  sum(time_to_count.values()))
  

def  display_all_results(result_readers):
  total = 0
  for reader in result_readers:
    for req in reader:
      total += req.e[5].i_val
  print "total of %d requests" % total

  
  
#  for k,v in sorted(code_to_count.items()):
#    print "Code %d:  %d" % (k, v)

if __name__ == '__main__':
    main()

