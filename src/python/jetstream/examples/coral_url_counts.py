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
  
  

if __name__ == '__main__':
    main()
