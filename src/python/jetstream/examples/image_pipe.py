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

INTERVAL = "interval"
HASH = "hash"
NONE = "none"

def main():

  parser = standard_option_parser()
  parser.add_option("--rate", dest="img_per_sec",
  default="2", help="number of images to send per second")
  parser.add_option("--dir", dest="dirname",
  default="sample_images", help="where to read from")
  parser.add_option("--prefix", dest="prefix", default = "l", help="prefix for images.")
  parser.add_option("--degradation", dest="deg",
  default="interval", help="which degradation to use; can be hash, interval")


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
  
  if options.deg == INTERVAL:
    print "Using interval sampling (Coarse-grained)"
  elif options.deg ==  HASH:
    print "Using hash-sampling. (Fine-grained)"
  elif options.deg == NONE:
    print "No degradation"
  else:
    print "unknown degradation %s. Aborting" % options.deg
    sys.exit(0)
    
  for node in all_nodes:
    if node == root_node and not options.generate_at_union:
      continue
    reader = jsapi.BlobReader(g, dirname=options.dirname, prefix=options.prefix, files_per_window=files_per_window, ms_per_window = 1000 * window_len_sec)
    if options.deg == INTERVAL:
      filter = jsapi.IntervalSampling(g, max_interval=4)
    elif options.deg ==  HASH:
      filter = jsapi.VariableSampling(g, field=0, type='I')
      filter.set_cfg("steps", "20")
      
    timestamp = jsapi.TimestampOperator(g, "ms")
    reader.instantiate_on(node)
    
    if options.deg == NONE:
      g.chain([reader, timestamp,  collector])
    else:
      g.chain([reader, filter, timestamp,  collector])
      
  print "deploying"
  deploy_or_dummy(options, server, g)


if __name__ == '__main__':
    main()
