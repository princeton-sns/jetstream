# A performance test. Can be single-process, two-process or one-process but with serialization

from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import time

from remote_controller import RemoteController
import query_graph as jsapi
from coral_util import *   #find_root_node, standard_option_parser,


def main():

  parser = standard_option_parser()
  parser.add_option("-r", "--rate", dest="rate",help="the rate to use per source (instead of rate schedule)")

  (options, args) = parser.parse_args()

  all_nodes,server = get_all_nodes(options)
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)

  g = get_graph(source_nodes, root_node,  options)

  deploy_or_dummy(options, server, g)
  
  
def get_graph(source_nodes, root_node, options):
  g = jsapi.QueryGraph()

  congest_logger = jsapi.AvgCongestLogger(g)
  congest_logger.instantiate_on(root_node)
  congest_logger.set_cfg("hist_field", 2)

  timestamp_cube_op= jsapi.TimestampOperator(g, "ms")
  timestamp_cube_op.instantiate_on(root_node)
  
  central_cube = g.add_cube("global_hists")
  central_cube.instantiate_on(root_node)
  central_cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, 0)
  central_cube.add_dim("dummydim", Element.INT32, 1)
  central_cube.add_agg("the_hist", jsapi.Cube.AggType.HISTO, 2)

  
  g.chain([congest_logger, timestamp_cube_op, central_cube] )

  add_latency_measure(g, central_cube, root_node, tti=4, hti=5, latencylog= options.latencylog)


  for node, i in numbered(source_nodes):
    sender = jsapi.RandHist(g)
    sender.instantiate_on(node)
    
    degrade = jsapi.DegradeSummary(g, 2)
    degrade.instantiate_on(node)
    
    timestamp_op= jsapi.TimestampOperator(g, "ms")
    hostname_extend_op = jsapi.ExtendOperator(g, "s", ["${HOSTNAME}"]) #used as dummy hostname for latency tracker
    hostname_extend_op.instantiate_on(node)
  

    g.chain( [sender, degrade, timestamp_op, hostname_extend_op, congest_logger])


  
  return g


if __name__ == '__main__':
    main()