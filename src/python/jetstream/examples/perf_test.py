#
# Graph data structure and algorithms for representing JetStream queries/computations.
#

from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import time


from remote_controller import RemoteController
import operator_graph as jsapi
from jetstream_types_pb2 import *


def main():

  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")

  parser.add_option("-2", "--two-nodes", dest="USE_TWO_NODES", action="store_true", 
                  help="whether to use two nodes", default=False)
  (options, args) = parser.parse_args()
  
  

  if ':' in options.controller:
    (serv_addr, serv_port) = options.controller.split(':')
    serv_port = int(serv_port)
  else:
    serv_addr = options.controller
    serv_port = 3456
  
  
  ### Define the graph abstractly, without a computation
  g = jsapi.OperatorGraph()
  source = jsapi.SendK(g, "1" + 10 * "0") #10 billion; fits into an int64 very easily
  sink = jsapi.RateRecord(g)
  
  g.connect(source,sink)
  #### Finished building in memory, now to join
  server = RemoteController()
  server.connect(serv_addr, serv_port)
  n = server.get_a_node()
  assert isinstance(n, NodeID)
  nodes = server.all_nodes()
  
  
  if options.USE_TWO_NODES:
    if len(nodes) < 2:
      print "not enough nodes for two-node test"
      sys.exit(0)
    source.instantiate_on(nodes[0])
    sink.instantiate_on(nodes[1])

  server.deploy(g)
    


if __name__ == '__main__':
    main()
