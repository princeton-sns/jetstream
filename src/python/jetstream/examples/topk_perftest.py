# Does windowed top-k for apache logs

from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import sys
import time
import types

from remote_controller import *
import query_graph as jsapi
from topk import get_graph

TIME_AT_RATE = 30 #seconds

def main():
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")
  parser.add_option("-d", "--dry-run", dest="DRY_RUN", action="store_true", 
                  help="shows PB without running", default=False)

  parser.add_option("-n", "--no-local", dest="NO_LOCAL", action="store_true", 
                  help="whether to do no local buffering", default=False)

  parser.add_option("-s", "--sampling", dest="SAMPLE", action="store_true", 
                  help="enables sampling, uses blocking instead for flow control", default=False)
  parser.add_option("-z", "--zipf", dest="zipf", action="store", type="float",
                  help="use zipf-distributed data with the given parameter")

  parser.add_option("-o", "--log_out_file", dest="perflog", 
                  help="file to log performance history into")
  
  parser.add_option("-l", "--latency_log_file", dest="latencylog", default="latencies.out",
                  help="file to log latency into")

  parser.add_option("-r", "--rate", dest="rate",help="the rate to use per source (instead of rate schedule)")
  parser.add_option("-u", "--union_root_node", dest="root_node",help="address of union/aggregator node")


  (options, args) = parser.parse_args()

  serv_addr, serv_port = normalize_controller_addr(options.controller)
  
    #Unlike most Jetstream programs, need to know how many nodes we have to set up the distribution properly
  server = RemoteController()
  server.connect(serv_addr, serv_port)
  all_nodes = server.all_nodes()
  if options.root_node:
    found = False
    for node in all_nodes:
      if node.address == options.root_node:
        root_node = node
        found = True 
        break
    if not found:
      print "Node with address: ",options.root_node," not found for use as the aggregator node"
      sys.exit()
    #root_addr, root_port =  normalize_controller_addr(options.root_node)
    #root_node = NodeID()
    #root_node.address = root_addr
    #root_node.portno = root_port
  else:
    root_node = server.get_a_node()
  assert isinstance(root_node, NodeID)
  
  print "Using",root_node,"as aggregator"
  #### Finished building in memory, now to deploy
  node_count = len(all_nodes)
  
#  for bw in [1000, 2000, 4000, 6000, 8000, 10000, 15000, 20000]:

  is_first_run = True
  if options.rate:
    schedule = [int(options.rate)]
  else:
    schedule = [15000, 25000, 50 * 1000, 75 * 1000, 150 * 1000, 250 * 1000]
  for bw in schedule:
    print "launching run with rate = %d per source (%d total)" % (bw, bw * node_count)
#    set_rate(g, bw)

    g = get_graph(root_node, all_nodes, rate=bw, perflog=options.perflog, clear_file=is_first_run,local_agg=(not options.NO_LOCAL), sample=options.SAMPLE, zipf=options.zipf, latencylog=options.latencylog)
    is_first_run=False

    req = g.get_deploy_pb()
    if options.DRY_RUN:
      print req
      break

    cid = server.deploy_pb(req)
    if type(cid) != types.IntType:
      break  

    print_wait()
    server.stop_computation(cid)
    time.sleep(10)   
    
  print "DONE; all computations should be stopped"
    #sleep a while; 
    #now stop and restart
    


def print_wait():
  for i in xrange(0,TIME_AT_RATE/3):    #sleep k seconds, waiting 3 seconds between printing dots
    time.sleep(3)   
    sys.stdout.write(".")
    sys.stdout.flush()
  sys.stdout.write("\n")
  
if __name__ == '__main__':
    main()
