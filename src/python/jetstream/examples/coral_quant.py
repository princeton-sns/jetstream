

from collections import defaultdict
import csv
from optparse import OptionParser 
import random
import sys
import time

from jetstream_types_pb2 import *
from remote_controller import *
import query_graph as jsapi

def main():

  parser = OptionParser()

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")
  parser.add_option("-d", "--dry-run", dest="DRY_RUN", action="store_true", 
                  help="shows PB without running", default=False)
  parser.add_option("-r", "--rate", dest="rate",help="the rate to use per source (instead of rate schedule)")
  parser.add_option("-u", "--union_root_node", dest="root_node",help="address of union/aggregator node")
  parser.add_option("-g", "--generate-at-union", dest="generate_at_union", action="store_false",help="generate data at union node", default=True)


  (options, args) = parser.parse_args()

  if options.DRY_RUN:
    id = NodeID()
    id.address ="somehost"
    id.portno = 12345
    all_nodes = [id]
  else:    
    server = RemoteController()
    server.connect(serv_addr, serv_port)
    all_nodes = server.all_nodes()


  serv_addr, serv_port = normalize_controller_addr(options.controller)

  root_node = find_root_node(options, all_nodes)

  g = jsapi.QueryGraph()

  req = g.get_deploy_pb()
  if options.DRY_RUN:
    print req
  else:
   server.deploy_pb(req)
    

def find_root_node(options, all_nodes):
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
  else:
    root_node = all_nodes[0]  #TODO randomize
  return root_node

if __name__ == '__main__':
    main()
    