#
# A JetStream controller.
#
# Note on threading: Built-in Python structures such as dict and list are themselves
# thread-safe (at least in CPython with the GIL), but access to the contents of the
# structures is not. For example, updating the list of workers is safe, but modifying
# the state of a worker is not (see liveness thread vs. handling heartbeat).
#

import asyncore
import asynchat

import logging
import socket
import struct
import subprocess
import sys
import threading
import time

from jetstream_types_pb2 import *
from controller_api import ControllerAPI
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
    """Returns a list of Workers"""
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

    
  def handle_alter (self, response, altertopo):
    response.type = ControlMessage.OK
    
    if len(self.workers) == 0:
      errorMsg = "No workers available to deploy the topology"
      logger.warning(errorMsg)
      response.type = ControlMessage.ERROR
      response.error_msg.msg = errorMsg
      return

    #TODO: The code below only deals with starting tasks. We also assume the client topology looks
    #like an in-tree from the stream sources to a global union point, followed by an arbitrary graph.

    # Find the first global union point, or the LCA of all sources.
    #taskID = self.findSourcesLCA(altertopo.toStart, altertopo.edges)

    # Set up the computation
    assert(len(altertopo.toStart) > 0)
    compID = altertopo.computationID
    assert(compID not in self.computations)
    comp = Computation(self, compID)
    self.computations[compID] = comp

    # Assign pinned operators to specified workers. For now, assign unpinned operators to a default worker.
    assignments = {}
    defaultEndpoint = self.get_one_node()
    for operator in altertopo.toStart:
      assert(operator.id.computationID == compID)
      endpoint = defaultEndpoint
      if operator.site.address != '':
        # Operator is pinned, so overwrite the target worker address
        endpoint = (operator.site.address, operator.site.portno)
      if endpoint not in assignments:
        assignments[endpoint] = self.workers[endpoint].create_assignment(compID)
      assignments[endpoint].operators.append(operator)

    # Repeat for cubes (temporary)
    for cube in altertopo.toCreate:
      endpoint = defaultEndpoint
      if cube.site.address != '':
        # Cube is pinned, so overwrite the target worker address
        endpoint = (cube.site.address, cube.site.portno)
      if endpoint not in assignments:
        assignments[endpoint] = self.workers[endpoint].create_assignment(compID)
      assignments[endpoint].cubes.append(cube)

    for endpoint,assignment in assignments.items():
      comp.assign_worker(endpoint, assignment)
      
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


  #TODO: Move this to within operator graph abstraction.
  def findSourcesLCA (self, tasks, edges):
    #TODO: For now just assert that the sources point to the same parent and return this parent
    #taskIdToTask = {}
    #for task in tasks:
    return -1


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
