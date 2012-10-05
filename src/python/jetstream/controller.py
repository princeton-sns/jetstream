#
# A JetStream controller.
#
# Note on thread-safety: Built-in Python structures like dict and list are themselves
# thread-safe (at least in CPython with the GIL), but access to the contents of the
# structures is not. For example, updating the list of workers is safe, but modifying
# the state of a worker is not (see liveness thread vs. handling heartbeat).
#

import asyncore
import asynchat

import logging
import re
import socket
import struct
import subprocess
import sys
import threading
import time

from jetstream_types_pb2 import *
from controller_api import ControllerAPI
from jsgraph import *
from computation_state import *
from generic_netinterface import JSServer
from server_http_interface import start_web_interface

from optparse import OptionParser 
#Could use ArgParse instead, but it's 2.7+ only.

logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  (options, args) = parser.parse_args()
  if options.config_file is not None:
    config = ConfigParser.ConfigParser()
    config.read(options.config_file)
    

  serv = get_server_on_this_node()
  start_web_interface(serv)
  serv.evtloop()

  
def get_server_on_this_node ():  
  bind_port = DEFAULT_BIND_PORT
  endpoint = ("", bind_port) #all interfaces?
  server = Controller(endpoint)
  return server


class Controller (ControllerAPI, JSServer):
  """A JetStream controller"""
  
  def __init__ (self, addr, hbInterval=CWorker.DEFAULT_HB_INTERVAL_SECS):
    JSServer.__init__ (self, addr)
    self.workers = {}
    self.computations = {}
    self.hbInterval = hbInterval
    self.running = False
    self.livenessThread = None
    # Given GIL, coarse-grained locking should be sufficient
    self.stateLock = threading.RLock()


  def start (self):
    self.running = True
    JSServer.start(self)
    # Start the liveness thread
    self.start_liveness_thread()


  def stop (self):
    self.running = False
    if self.livenessThread:
      self.livenessThread.join()
    JSServer.stop(self)


  def start_liveness_thread (self):
    assert(self.livenessThread == None)
    self.livenessThread = threading.Thread(group=None, target=self.liveness_thread, args=())
    self.livenessThread.daemon = True
    self.livenessThread.start()


  def liveness_thread (self):
    while self.running:
      self.stateLock.acquire()
      for w,s in self.workers.items():
        # TODO: Just delete the node for now, but going forward we'll have to 
        # reschedule computations etc.
        if s.update_state() == CWorker.DEAD:
          logger.info("marking worker %s:%d as dead due to timeout" % (w[0],w[1]))
          # This is thread-safe since we are using items() and not an iterator
          del self.workers[w]
      self.stateLock.release()
          
      # All workers reporting to a controller should have the same hb interval
      # (enforced via common config file)
      time.sleep(self.hbInterval)


  def get_nodes (self):
    """Returns a list of Workers.
     Return type is list of tuples, each of which is address, port"""
    res = []
    res.extend(self.workers.values())
    return res

    
  def get_one_node (self):
    res = self.workers.keys()[0]
    return res


  def serialize_nodeList (self, nodes):
    """Serialize node list as list of protobuf NodeIDs"""
    res = []
    for node in nodes:
      nID = NodeID()
      nID.address,nID.portno = node.endpoint
      res.append(nID)
    return res


  def handle_get_nodes (self, response):
    nodeList = self.get_nodes()
    response.type = ControlMessage.NODES_RESPONSE
    response.nodes.extend(self.serialize_nodeList(nodeList))
    response.node_count = len(nodeList)

    
  def handle_heartbeat (self, hb, clientEndpoint):
    t = long(time.time())
    print "got heartbeat at %s from sender %s" % (time.ctime(t), str(clientEndpoint))
    self.stateLock.acquire()
    if clientEndpoint not in self.workers:
      logger.info("Added worker %s" % (str(clientEndpoint)))
      self.workers[clientEndpoint] = CWorker(clientEndpoint, self.hbInterval)
    self.workers[clientEndpoint].receive_hb(hb)
    self.stateLock.release()


  CUBE_NAME_PAT = re.compile("[a-zA-Z0-9_]+$")
  def validate_topo(self,altertopo):
    """Validates a topology. Should return an empty string if valid, else an error message."""
    
    #Organization of this method is parallel to the altertopo structure.
  # First verify top-level metadata. Then operators, then cubes.
    if altertopo.computationID in self.computations:
      return "computation ID %d already in use" % altertopo.computationID

    if len(altertopo.toStart) == 0:
      return "Topology includes no operators"

#  Can't really do this verification -- breaks with UDFs
#    for operator in altertopo.toStart:
#      if not operator.op_typename in KNOWN_OP_TYPES:
#        print "WARNING: unknown operator type KNOWN_OP_TYPES"
      
    for cube in altertopo.toCreate:
      if not self.CUBE_NAME_PAT.match(cube.name):
        return "invalid cube name %s" % cube.name
      if len(cube.schema.aggregates) == 0:
        return "cubes must have at least one aggregate per cell"
      if len(cube.schema.dimensions) == 0:
        return "cubes must have at least one dimension"

    return ""
    
    
  def handle_alter (self, response, altertopo):
    response.type = ControlMessage.OK
    
    if len(self.workers) == 0:
      errorMsg = "No workers available to deploy the topology"
      logger.warning(errorMsg)
      response.type = ControlMessage.ERROR
      response.error_msg.msg = errorMsg
      return # Note that we modify response in-place. (ASR: FIXME; why do it this way?)

    err = self.validate_topo(altertopo)
    if len(err) > 0:
      print "Invalid topology:",err
      response.type = ControlMessage.ERROR
      response.error_msg.msg = err
      return
      
    #TODO: The code below only deals with starting tasks.
    #TODO: Make these todos bitbucket issues or move to assumption comments at top

    # Build the computation graph so we can analyze/manipulate it
    jsGraph = JSGraph(altertopo.toStart, altertopo.toCreate, altertopo.edges)
    # Set up the computation
    compID = altertopo.computationID
    comp = Computation(self, compID, jsGraph)
    self.computations[compID] = comp

    assignments = {}
    #TODO: Sid will consolidate with Computation data structure
    taskLocations = {}
    sources = jsGraph.get_sources()
    sink = jsGraph.get_sink()
    defaultEndpoint = self.get_one_node()

    # Assign pinned nodes to their specified workers. If a source or sink is unpinned,
    # assign it to a default worker.
    toPin = []
    toPin.extend(altertopo.toStart)
    toPin.extend(altertopo.toCreate)
    for node in toPin:
      endpoint = None
      if node.site.address != '':
        # Node is pinned to a specific worker
        endpoint = (node.site.address, node.site.portno)
      elif (node in sources) or (node == sink):
        # Node is an unpinned source/sink; pin it to a default worker
        endpoint = defaultEndpoint
      else:
        # Node will be placed later
        continue
      if endpoint not in assignments:
        assignments[endpoint] = self.workers[endpoint].create_assignment(compID)
      assignments[endpoint].add_node(node)
      #TODO: Sid will consolidate with Computation data structure
      nodeId = node.id.task if isinstance(node, TaskMeta) else node.name
      taskLocations[nodeId] = endpoint
    
    # Find the first global union node, aka the LCA of all sources.
    union = jsGraph.get_sources_lca()
    # All nodes from union to sink should be at one site
    nodeId = union.id.task if isinstance(union, TaskMeta) else union.name
    endpoint = taskLocations[nodeId] if nodeId in taskLocations else defaultEndpoint
    if endpoint not in assignments:
      assignments[endpoint] = self.workers[endpoint].create_assignment(compID)
    toPlace = jsGraph.get_descendants(union)
    for node in toPlace:
      assignments[endpoint].add_node(node)
    # All nodes from source to union should be at one site, for each source
    for source in sources:
      nodeId = source.id.task if isinstance(source, TaskMeta) else source.name
      assert(nodeId in taskLocations)
      endpoint = taskLocations[nodeId]
      toPlace = jsGraph.get_descendants(source, union)
      for node in toPlace:
        assignments[endpoint].add_node(node)

    # Finalize the worker assignments
    for endpoint,assignment in assignments.items():
      comp.assign_worker(endpoint, assignment)
    
    comp.add_edges(altertopo.edges)
    
    # Start the computation
    logger.info("Starting computation %d" % (compID))
    comp.start()


  def handle_alter_response (self, altertopo, workerEndpoint):
    #TODO: As above, the code below only deals with starting tasks
    
    compID = altertopo.computationID
    if compID not in self.computations:
      print "WARNING: Invalid computation id %d in ALTER_RESPONSE message" % (compID)
      return
    # Let the computation know which parts of the assignment were started/created
    actualAssignment = WorkerAssignment(altertopo.computationID, altertopo.toStart, altertopo.toCreate)
    self.computations[compID].update_worker(workerEndpoint, actualAssignment)


  def process_message (self, buf, handler):
    """Processes control messages; only supports single-threaded access"""
    req = ControlMessage()
    req.ParseFromString(buf)
#    print ("server got %d bytes," % len(buf)), req
    response = ControlMessage()
    # Assign a default response type, should be overwritten below
    response.type = ControlMessage.ERROR
    
    if req.type == ControlMessage.GET_NODE_LIST_REQ:
      self.handle_get_nodes(response)
    elif req.type == ControlMessage.ALTER:
      self.handle_alter(response, req.alter)
    elif req.type == ControlMessage.ALTER_RESPONSE:
      self.handle_alter_response(req.alter, handler.cli_addr)
      return  # no response
    elif req.type == ControlMessage.HEARTBEAT:
      self.handle_heartbeat(req.heartbeat, handler.cli_addr)
      return  # no response
    elif req.type == ControlMessage.OK:
      # This should not be used
      logger.fatal("Received dangling OK message from %s" % (str(handler.cli_addr)))
      assert(false)
      return  # no response
    else:
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "unknown error"

    handler.send_pb(response)


if __name__ == '__main__':
  main()
  sys.exit(0)
