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
from client_reader import ClientDataReader


from coral_parse import coral_fnames,coral_fidxs, coral_types
from coral_util import *   #find_root_node, standard_option_parser,

logger = logging.getLogger('JetStream')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

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
    make_graph = make_counts_query_graph
    display_results = display_respcode_results
  elif mode == "by_time":
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
  for node in all_nodes:
    reader = ClientDataReader()

    g= jsapi.QueryGraph()
    last_op = make_graph(g, node)
    
    last_op.instantiate_on(node)
    g.connectExternal(last_op, reader.prep_to_receive_data())
    result_readers.append(reader)
    if options.DRY_RUN:
      req = g.get_deploy_pb()
      planner = QueryPlanner( {("somehost", 12345): ("somehost", 12346) } )
      planner.take_raw_topo(req.alter)
      planner.get_assignments(1)
      print req
      sys.exit(0)
    server.deploy(g)
  
  completed = 0
  t = 0
  MS_PER_TICK = 20
  TIME_TO_WAIT = 20  # seconds
  if options.wait:
    TIME_TO_WAIT = int (options.wait)
  TICKS_TO_WAIT = 1000 * TIME_TO_WAIT / MS_PER_TICK
  while t < TICKS_TO_WAIT and completed < num_nodes:
    time.sleep( MS_PER_TICK / 1000.0 )
    t += 1
    completed = sum( [r.is_finished for r in result_readers])
    tuples = sum( [r.tuples_received for r in result_readers])
    if t % 10 == 0:
      logger.info("tick; %d readers completed. %d total tuples." % (completed, tuples))

  logger.info("finished. %d readers completed. %d total tuples. Total time taken was %d ms" % (completed, tuples, t * MS_PER_TICK))
  if completed == 0:
    sys.exit(0)
  display_results([r for r in result_readers if r.is_finished])

def make_trivial_graph(g, node):
     
  echoer = jsapi.SendK(g, 40)
  return echoer


def get_simple_qgraph(g, node):
  cube = g.add_cube("local_records")
  cube.instantiate_on(node)
  define_schema_for_raw_cube (cube)
  cube.set_overwrite(False)
  
  pull_from_local = jsapi.OneShotSubscriber(g, filter={})
  g.connect(cube, pull_from_local)
  return pull_from_local
  

def make_counts_query_graph(g, node):
  print "returning total counts by response code"
  pull_from_local = get_simple_qgraph(g,node)
  pull_from_local.set_cfg("rollup_levels", "0,1,0")  #roll up everything but response code
  return pull_from_local


def  display_respcode_results(result_readers):
  code_to_count = defaultdict(int)
  for reader in result_readers:
    for t in reader:
      code = t.e[1].i_val
      val = t.e[5].i_val
      code_to_count[code] += val
  
  for k,v in sorted(code_to_count.items()):
    print "Code %d:  %d" % (k, v)


def make_time_query_graph(g, node):
  print "computing counts by time"
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

