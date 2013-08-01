from collections import defaultdict
from optparse import OptionParser
import random
import sys
import time

from jetstream_types_pb2 import *
from remote_controller import *
import query_graph as jsapi
from query_planner import QueryPlanner

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
  root_node = find_root_node(options, all_nodes)
  
  print "%d worker nodes in system" % len(all_nodes)
  g= jsapi.QueryGraph()
  
  collector = jsapi.AvgCongestLogger(g)
  collector.instantiate_on(root_node)
  
  for node in all_nodes:
    if node == root_node:
      continue
    reader = jsapi.BlobReader(g, dirname="sample_images", prefix="l", ms_per_file="500")
    filter = jsapi.IntervalSampling(g, max_interval=4)
    reader.instantiate_on(node)
    
    g.chain([reader, filter, collector])
  print "deploying"
  deploy_or_dummy(options, server, g)


if __name__ == '__main__':
    main()
