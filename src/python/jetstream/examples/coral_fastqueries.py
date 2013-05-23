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
  (options, args) = parser.parse_args()

  all_nodes,server = get_all_nodes(options)
  num_nodes = len(all_nodes)
  result_readers = []
  for node in all_nodes:
    reader = ClientDataReader()

    g= jsapi.QueryGraph()

    parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
       coral_fidxs['URL_requested'], coral_fidxs['nbytes'], coral_fidxs['dl_utime'], len(coral_types) ]
    echoer = jsapi.SendK(g, 40)
    g.connectExternal(echoer, reader.prep_to_receive_data())
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


if __name__ == '__main__':
    main()

