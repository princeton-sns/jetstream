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

WINDOW_SECS = 3
OFFSET = 3000   #ms
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



  parser.add_option("-o", "--log_out_file", dest="perflog", 
                  help="file to log performance history into")

  (options, args) = parser.parse_args()

  serv_addr, serv_port = normalize_controller_addr(options.controller)
  
    #Unlike most Jetstream programs, need to know how many nodes we have to set up the distribution properly
  server = RemoteController()
  server.connect(serv_addr, serv_port)
  root_node = server.get_a_node()
  assert isinstance(root_node, NodeID)
  all_nodes = server.all_nodes()
  
  print "Using",root_node,"as aggregator"
  #### Finished building in memory, now to deploy
  node_count = len(all_nodes)
  
#  for bw in [1000, 2000, 4000, 6000, 8000, 10000, 15000, 20000]:
  for bw in [15000, 25000, 50 * 1000, 75 * 1000, 150 * 1000, 250 * 1000]:
    print "launching run with rate = %d per source (%d total)" % (bw, bw * node_count)
#    set_rate(g, bw)
    g = get_graph(root_node, all_nodes, options, bw)

    req = g.get_deploy_pb()
    if options.DRY_RUN:
      print req
      break

    cid = server.deploy_pb(req)
    if type(cid) == types.IntType:
      print time.ctime(),"Computation running; ID =",cid
    else:
      print "computation failed",cid
      break  

    print_wait()
    server.stop_computation(cid)
    time.sleep(10)   
    
  print "DONE; all computations should be stopped"
    #sleep a while; 
    #now stop and restart
    
is_first_run = False
def get_graph(root_node, all_nodes, options, rate=1000):
  global is_first_run
  ### Define the graph abstractly
  g = jsapi.QueryGraph()
  LOCAL_AGG = not options.NO_LOCAL
  

  final_cube = g.add_cube("final_results")
  final_cube.add_dim("state", Element.STRING, 0)
  final_cube.add_dim("time", Element.TIME, 1)
  final_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 3)
  if LOCAL_AGG:
    final_cube.add_agg("source", jsapi.Cube.AggType.STRING, 4)
  
  final_cube.set_overwrite(True)  #fresh results  

  final_cube.instantiate_on(root_node)

  if options.SAMPLE:
    sampling_balancer =jsapi.SamplingController(g)
    sampling_balancer.instantiate_on(root_node)
    g.connect(sampling_balancer, final_cube)  
  

  pull_op = jsapi.TimeSubscriber(g, {}, 1000, "-count") #pull every second
  pull_op.set_cfg("ts_field", 1)
  pull_op.set_cfg("window_offset", OFFSET) #but trailing by four
  
  eval_op = jsapi.RandEval(g)
  if options.perflog:
    eval_op.set_cfg("file_out", options.perflog)
    if is_first_run:
      eval_op.set_cfg("append", "false")
      is_first_run = False
  
  latency_measure_op = jsapi.LatencyMeasureSubscriber(g, 2, 4, 100);
  echo_op = jsapi.Echo(g);

  g.connect(final_cube, pull_op)  
  g.connect(pull_op, eval_op)
  
  g.connect(final_cube, latency_measure_op)  
  g.connect(latency_measure_op, echo_op)  
  
  
  n = len(all_nodes)
  for node,k in zip(all_nodes, range(0,n)):
    src = jsapi.RandSource(g, n, k) #tuple: state, time
    src.set_cfg("rate", rate)
    timestamp_op= jsapi.TimestampOperator(g, "ms") 
    count_extend_op = jsapi.ExtendOperator(g, "i", ["1"])
    node_num_op = jsapi.ExtendOperator(g, "s", ["node"+str(k)]) #not hostname for debugging locally
    g.connect(src, timestamp_op)
    g.connect(timestamp_op, count_extend_op)
    g.connect(count_extend_op, node_num_op)
    #tuple format: 0=>S-state, 1=>T-time, 2=>D-timestamp(ms), 3=>I-count, 4=>S-node#

    round_op = jsapi.TRoundOperator(g, fld=1, round_to=WINDOW_SECS)

    if LOCAL_AGG:   # local aggregation
      node_num_local_op = jsapi.ExtendOperator(g, "s", ["node"+str(k)])

      local_cube = g.add_cube("local_results_%d" % k)
      local_cube.add_dim("state", Element.STRING, 0)
      local_cube.add_dim("time", Element.TIME, 1)
      local_cube.add_agg("min_timestamp", jsapi.Cube.AggType.MIN_D, 2)
      local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 3)
      local_cube.set_overwrite(True)  #fresh results
      #cube output tuple is 0=>S-state, 1=>T-time, 2=>D-timestamp(ms), 3=>I-count
      pull_op = jsapi.TimeSubscriber(g, {}, WINDOW_SECS * 1000)
      pull_op.set_cfg("ts_field", 1)
      pull_op.set_cfg("window_offset", OFFSET) #pull every three seconds, trailing by one
      g.connect(node_num_op, local_cube)
      g.connect(local_cube, pull_op)      
      g.connect(pull_op, node_num_local_op)
      local_cube.instantiate_on(node)    
      #tuple format: 0=>S-state, 1=>T-time, 2=>D-timestamp(ms), 3=>I-count, 4=>S-node#
      last_local = node_num_local_op
    else:
      last_local = node_num_op
    
    src.instantiate_on(node)
    
    if options.SAMPLE:  
      sample_op = jsapi.VariableSampling(g)
      g.connect(last_local, sample_op)
      g.connect(sample_op, round_op)
      g.connect(round_op, sampling_balancer)
    else:
      g.connect(last_local, round_op)
      g.connect(round_op, final_cube)
  return g


def print_wait():
  for i in xrange(0,TIME_AT_RATE/3):    #sleep k seconds, waiting 3 seconds between printing dots
    time.sleep(3)   
    sys.stdout.write(".")
    sys.stdout.flush()
  sys.stdout.write("\n")
  
if __name__ == '__main__':
    main()
