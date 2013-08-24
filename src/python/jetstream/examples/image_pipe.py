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

window_len_sec = 1.0

def main():

  parser = standard_option_parser()
  parser.add_option("--rate", dest="img_per_sec",
  default="2", help="number of images to send per second")
  parser.add_option("--dir", dest="dirname",
  default="sample_images", help="where to read from")
  parser.add_option("--prefix", dest="prefix", default = "l", help="prefix for images.")

  (options, args) = parser.parse_args()

  all_nodes,server = get_all_nodes(options)
  root_node = find_root_node(options, all_nodes)
  
  print "%d worker nodes in system" % len(all_nodes)
  g= jsapi.QueryGraph()
  files_per_window = float(options.img_per_sec) * window_len_sec
  collector = jsapi.ImageQuality(g)
  collector.instantiate_on(root_node)

  if len(all_nodes) < 1 or (len(all_nodes) == 1 and options.generate_at_union):
    print "FAIL: not enough nodes"
    sys.exit(0)
  
  for node in all_nodes:
    if node == root_node and not options.generate_at_union:
      continue
    reader = jsapi.BlobReader(g, dirname=options.dirname, prefix=options.prefix, files_per_window=files_per_window, ms_per_window = 1000 * window_len_sec)
    filter = jsapi.IntervalSampling(g, max_interval=4)
    timestamp = jsapi.TimestampOperator(g, "ms")
    reader.instantiate_on(node)
    
    g.chain([reader, filter, timestamp,  collector])
  print "deploying"
  deploy_or_dummy(options, server, g)


if __name__ == '__main__':
    main()
