# Does windowed top-k for apache logs

from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import time

from remote_controller import RemoteController
import query_graph as jsapi

WINDOW_SECS = 3

def main():
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")
  parser.add_option("-d", "--dry-run", dest="DRY_RUN", action="store_true", 
                  help="whether to use two nodes", default=False)


  (options, args) = parser.parse_args()

  if ':' in options.controller:
    (serv_addr, serv_port) = options.controller.split(':')
    serv_port = int(serv_port)
  else:
    serv_addr = options.controller
    serv_port = 3456
  
    #Unlike most Jetstream programs, need to know how many nodes we have to set up the distribution properly
  server = RemoteController()
  server.connect(serv_addr, serv_port)
  root_node = server.get_a_node()
  assert isinstance(root_node, NodeID)
  all_nodes = server.all_nodes()
  
  
  ### Define the graph abstractly
  g = jsapi.QueryGraph()


  final_cube = g.add_cube("final_results")
  final_cube.add_dim("state", Element.STRING, 0)
  final_cube.add_dim("time", Element.TIME, 1)
  final_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
  final_cube.add_agg("sources", jsapi.Cube.AggType.STRING, 3)
  
  final_cube.set_overwrite(True)  #fresh results  

  pull_op = jsapi.TimeSubscriber(g, {}, 1000, "-count") #pull every second
  pull_op.set_cfg("ts_field", 1)
  pull_op.set_cfg("window_offset", 4000) #but trailing by four
  
  eval_op = jsapi.RandEval(g)
  g.connect(final_cube, pull_op)  
  g.connect(pull_op, eval_op)
  
  eval_op.instantiate_on(root_node)
  
  n = len(all_nodes)
  for node,k in zip(all_nodes, range(0,n)):
    src = jsapi.RandSource(g, n, k)
    src.set_cfg("rate", 1000)

    local_cube = g.add_cube("local_results")
    local_cube.add_dim("state", Element.STRING, 0)
    local_cube.add_dim("time", Element.TIME, 1)
    local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 2)
    local_cube.set_overwrite(True)  #fresh results

    pull_op = jsapi.TimeSubscriber(g, {}, WINDOW_SECS * 1000)
    pull_op.set_cfg("ts_field", 1)
    pull_op.set_cfg("window_offset", 1000) #pull every three seconds, trailing by one
    
    extend_op = jsapi.ExtendOperator(g, "s", ["node"+str(k)])
    round_op = jsapi.TRoundOperator(g, fld=1, round_to=5)


    g.connect(src, local_cube)  
    g.connect(local_cube, pull_op)
    g.connect(pull_op, extend_op)
    g.connect(extend_op, round_op)

    round_op.instantiate_on(node)

    g.connect(round_op, final_cube)
  
  #### Finished building in memory, now to deploy
  
  req = g.get_deploy_pb()
  print req
  if not options.DRY_RUN:
    server.deploy_pb(req)
    


if __name__ == '__main__':
    main()
