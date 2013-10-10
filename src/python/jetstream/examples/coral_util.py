import os
import os.path
import pickle
import sys
import time

from optparse import OptionParser
from remote_controller import *
import query_graph as jsapi
from query_planner import QueryPlanner


def parse_ts(start_ts):
  if start_ts is None:
    t = time.time()
    print "no start-time specified; using %d" % t
    return long(t)
  if start_ts.isdigit():
    return int(start_ts)
  else:
    #todo could allow more formats here
    assert False

def standard_option_parser():
  parser = OptionParser()
  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")
  parser.add_option("-d", "--dry-run", dest="DRY_RUN", action="store_true",
                  help="shows PB from dummy topology without running", default=False)
  parser.add_option("--display", dest="SHOW_TOPO", action="store_true",
                  help="shows PB", default=False)                  
  parser.add_option("-u", "--union_root_node", dest="root_node",help="address of union/aggregator node")
  parser.add_option("-f", "--file_name", dest="fname",help="name of input file")
  parser.add_option("-g", "--generate-at-union", dest="generate_at_union", action="store_true",help="generate data at union node", default=False)
  parser.add_option("-l", "--latency_log_file", dest="latencylog",
  default="latencies.out", help="file to log latency into")
  parser.add_option("--start-time", dest="start_ts",
  default=None, help="unix timestamp to start simulation at")
  parser.add_option("-n", "--number-sources", dest="num_srcs",
  default=None, help="Number of source nodes to use")
  parser.add_option("--timewarp", dest="warp_factor",
  default="1", help="simulation speedup")
  parser.add_option("--analyze_only", dest="analyze_only",
  action="store_true", default=False)
  parser.add_option("--load_only", dest="load_only",
  action="store_true", default=False)
  parser.add_option("--bw_cap", dest="bw_cap",
  action="store", help="bw cap in kb/sec/link")
  parser.add_option("--no_echo", dest="no_echo",
  action="store_true", default=False)
  parser.add_option("--no_backoff", dest="no_backoff",
  action="store_true", default=False)
  parser.add_option("--max-rollup", dest="max_rollup",
  default = "30")
  return parser


def get_all_nodes(options):
  if options.DRY_RUN:
    id = NodeID()
    id.address ="somehost"
    id.portno = 12345
    id2 = NodeID()
    id2.address = "otherhost"
    id2.portno = 12345
    all_nodes = [id, id2]
    server = None
  else:
    serv_addr, serv_port = normalize_controller_addr(options.controller)
    server = RemoteController()
    server.connect(serv_addr, serv_port)
    all_nodes = server.all_nodes()
    
  all_nodes.sort(key=lambda x: x.address)
#  print "node list is",all_nodes    
  return all_nodes,server

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

  print "Using %s:%d as root aggregator" % (root_node.address, root_node.portno)

  return root_node

def get_source_nodes(options, all_nodes, root_node):
  source_nodes = all_nodes
  if not options.generate_at_union:
    print "Removing aggregator from list of nodes"
    source_nodes.remove(root_node)

  if options.num_srcs is not None:
    source_nodes = source_nodes[0:options.num_srcs]

  return source_nodes

CACHE_PATH = "host_numbering.cache"
def numbered(all_nodes, can_read_cache = True):
  
  p = os.path.normpath(CACHE_PATH)
  if os.path.exists(p) and can_read_cache:
    print "reading cached cube numbering from", p
    f = open(p, 'rb')
    mapping = dict(pickle.load( f ))
    f.close()
    return [ (n, mapping.get(n.address, "x")) for n in all_nodes]
  
  else:
    res = zip(all_nodes, range(0, len(all_nodes)))
    unfolded_res = [ (n.address, num) for (n, num) in res]
    f = open(p, 'wb')
    pickle.dump(unfolded_res, f)
    f.close()
  
  return res


def deploy_or_dummy(options, server, g):
  req = g.get_deploy_pb()
  ops = len(req.alter[0].toStart)
  cubes = len(req.alter[0].toCreate)
  if options.DRY_RUN:
    planner = QueryPlanner( {("somehost", 12345): ("somehost", 12346), \
            ("otherhost", 12345): ("otherhost", 12346) } )
    planner.take_raw_topo(req.alter[0])
    planner.get_assignments(1)
    print req
    sys.exit(0)
  else:
    if options.SHOW_TOPO:
        print req
    server.deploy_pb(req)
  print "Job has %d cubes and %d operators." % (cubes, ops)


def  add_latency_measure(g, central_cube, root_node, tti, hti, latencylog, interval=100):
  #time tuple-index and host tuple-index, 
  latency_measure_op = jsapi.LatencyMeasureSubscriber(g, tti, hti, interval_ms=interval)
  latency_measure_op.instantiate_on(root_node)
  echo_op = jsapi.Echo(g);
  echo_op.set_cfg("file_out", latencylog)
  echo_op.instantiate_on(root_node)
  g.chain([central_cube, latency_measure_op, echo_op])


def define_raw_cube(g, cube_name, cube_node, ids = [0,1,2,3,4,5,6,7], overwrite=False):
  cube = g.add_cube(cube_name)
  cube.instantiate_on(cube_node)
  cube.set_overwrite(overwrite)
  cube.add_dim("time", CubeSchema.Dimension.TIME_CONTAINMENT, ids[0])
  cube.add_dim("response_code", CubeSchema.Dimension.INT32, ids[1])
  cube.add_dim("url", CubeSchema.Dimension.STRING, ids[2])
#  cube.add_dim("referer", CubeSchema.Dimension.STRING, ids[3])
  cube.add_agg("size", jsapi.Cube.AggType.COUNT, ids[4])  #effectively a sum
  cube.add_agg("latency", jsapi.Cube.AggType.COUNT, ids[5]) # effectively a sum
  cube.add_agg("count", jsapi.Cube.AggType.COUNT, ids[6])
  return cube