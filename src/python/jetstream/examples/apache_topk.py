# Does windowed top-k for apache logs

from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import time

from remote_controller import RemoteController,normalize_controller_addr
import query_graph as jsapi

def main():
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")

  (options, args) = parser.parse_args()

  serv_addr,serv_port =  normalize_controller_addr(options.controller)
  
  file_to_parse = args[0]
  
  k2 = 20 #how many to pull to top level
  k = 10 #how many to display
  
  ### Define the graph abstractly, without a computation
  g = jsapi.QueryGraph()
  reader = jsapi.FileRead(g, file_to_parse)
  parse = jsapi.GenericParse(g, ".*GET ([^ ]*) .*", "s")

  
  local_cube = g.add_cube("local_results")
  local_cube.add_dim("url", Element.STRING, 0)
#  cube.add_dim("hostname", Element.STRING, 1)
  local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
  local_cube.set_overwrite(True)  #fresh results

  
  pull_k2 = jsapi.TimeSubscriber(g, {}, 2000, "-count", k2)
  echo = jsapi.Echo(g)
  
  
#  local_cube = jsapi.Echo(g)
  
  g.connect(reader,parse)
  g.connect(parse, local_cube)
  g.connect(local_cube, pull_k2)
  g.connect(pull_k2,echo)


  
#  Should do a pull into a consolidated cube  
  
  #### Finished building in memory, now to join
  server = RemoteController((serv_addr, serv_port))

  n = server.get_a_node()
  assert isinstance(n, NodeID)
  all_nodes = server.all_nodes()
  
  local_cube.instantiate_on(all_nodes)
    
  server.deploy(g)
    


if __name__ == '__main__':
    main()
