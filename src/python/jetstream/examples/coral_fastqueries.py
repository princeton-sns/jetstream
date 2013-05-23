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
  (options, args) = parser.parse_args()
  
  if options.mode == "trivial":
    make_graph = make_trivial_graph
    display_results = lambda x: x
  elif options.mode == "counts":
    make_graph = make_counts_query_graph
    display_results = display_respcode_results
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
    server.deploy(g)
  
  completed = 0
  t = 0
  MS_PER_TICK = 100
  TIME_TO_WAIT = 10  # seconds
  TICKS_TO_WAIT = 1000 * TIME_TO_WAIT / MS_PER_TICK
  while t < TICKS_TO_WAIT and completed < num_nodes:
    time.sleep( MS_PER_TICK / 1000.0 )
    t += 1
    completed = sum( [r.is_finished for r in result_readers])
    tuples = sum( [r.tuples_received for r in result_readers])
    if t % 10 == 0:
      logger.info("tick; %d readers completed. %d total tuples." % (completed, tuples))

  logger.info("finished. %d readers completed. %d total tuples. Total time taken was %d ms" % (completed, tuples, t * MS_PER_TICK))
  display_results(result_readers)

def make_trivial_graph(g, node):
     
  echoer = jsapi.SendK(g, 40)
  return echoer


def make_counts_query_graph(g, node):
  print "returning total counts by response code"

  cube = g.add_cube("local_records")
  cube.instantiate_on(node)
  define_schema_for_raw_cube (cube)
  cube.set_overwrite(False)
  
  pull_from_local = jsapi.OneShotSubscriber(g, filter={})
  pull_from_local.set_cfg("rollup_levels", "0,1,0")  #roll up everything but response code
  g.connect(cube, pull_from_local)
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


if __name__ == '__main__':
    main()

