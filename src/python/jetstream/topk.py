from jetstream_types_pb2 import *
import query_graph as jsapi

WINDOW_SECS = 3
OFFSET = 3000   #ms

def get_graph(root_node, all_nodes, rate=1000, perflog="", clear_file = False, latencylog="latencies.out", local_agg=False, sample=False, zipf=0):
  ### Define the graph abstractly

  g = jsapi.QueryGraph()
  

  final_cube = g.add_cube("final_results")
  final_cube.add_dim("state", Element.STRING, 0)
  final_cube.add_dim("time", Element.TIME, 1)
  final_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 3)
  if local_agg:
    final_cube.add_agg("source", jsapi.Cube.AggType.STRING, 4)
  
  final_cube.set_overwrite(True)  #fresh results  

  final_cube.instantiate_on(root_node)

  if sample:
    sampling_balancer =jsapi.SamplingController(g)
    sampling_balancer.instantiate_on(root_node)
    g.connect(sampling_balancer, final_cube)  
  

  pull_op = jsapi.TimeSubscriber(g, {}, 1000, "-count") #pull every second
  pull_op.set_cfg("ts_field", 1)
  pull_op.set_cfg("window_offset", OFFSET) #but trailing by a few
  
  eval_op = jsapi.RandEval(g)
  if perflog:
    eval_op.set_cfg("file_out", perflog)
    if clear_file:
      eval_op.set_cfg("append", "false")
  if zipf > 0:
    eval_op.set_cfg("mode", "zipf")
    eval_op.set_cfg("zipf_param", zipf)


  
  latency_measure_op = jsapi.LatencyMeasureSubscriber(g, 2, 4, 100);
  latency_measure_op.instantiate_on(root_node)
  echo_op = jsapi.Echo(g);
  echo_op.set_cfg("file_out", latencylog)
  echo_op.instantiate_on(root_node)

  g.connect(final_cube, pull_op)  
  g.connect(pull_op, eval_op)
  
  g.connect(final_cube, latency_measure_op)  
  g.connect(latency_measure_op, echo_op)  
  
  
  n = len(all_nodes)
  for node,k in zip(all_nodes, range(0,n)):
    src = jsapi.RandSource(g, n, k) #tuple: state, time
    src.set_cfg("rate", rate)
    if zipf > 0:
      src.set_cfg("mode", "zipf")
      src.set_cfg("zipf_param", zipf)
    timestamp_op= jsapi.TimestampOperator(g, "ms") 
    count_extend_op = jsapi.ExtendOperator(g, "i", ["1"])
    node_num_op = jsapi.ExtendOperator(g, "s", ["node"+str(k)]) #not hostname for debugging locally
    g.connect(src, timestamp_op)
    g.connect(timestamp_op, count_extend_op)
    g.connect(count_extend_op, node_num_op)
    #tuple format: 0=>S-state, 1=>T-time, 2=>D-timestamp(ms), 3=>I-count, 4=>S-node#

    round_op = jsapi.TRoundOperator(g, fld=1, round_to=WINDOW_SECS)

    if local_agg:   # local aggregation
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
    
    if sample:  
      sample_op = jsapi.VariableSampling(g)
      g.connect(last_local, sample_op)
      g.connect(sample_op, round_op)
      g.connect(round_op, sampling_balancer)
    else:
      g.connect(last_local, round_op)
      g.connect(round_op, final_cube)
  return g

