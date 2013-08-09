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
import ConfigParser

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
from query_planner import QueryPlanner


from optparse import OptionParser 
#Could use ArgParse instead, but it's 2.7+ only.

NOSECTION = 'nosection'
class FakeSecHead(object):
   def __init__(self, fp):
     self.fp = fp
     self.sechead = '[' + NOSECTION + ']\n'
   def readline(self):
     if self.sechead:
       try: return self.sechead
       finally: self.sechead = None
     else: return self.fp.readline()

logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  (options, args) = parser.parse_args()

  config = ConfigParser.SafeConfigParser( {'controller_web_port': "8082", \
               'controller_ep':""} )
  
  if options.config_file is not None:
    fp = open(options.config_file)
    config.readfp( FakeSecHead (fp))
    fp.close()
  else:
    config.add_section(NOSECTION)
  

  addr = config.get(NOSECTION, 'controller_ep')
  if len(addr) > 0:
    endpoint, bind_port = addr.split(':')
    bind_port = int(bind_port)
  else:
    (endpoint,bind_port) = "", DEFAULT_BIND_PORT
  
  serv = get_server_on_this_node(endpoint, bind_port)
  start_web_interface(serv, endpoint, config.getint(NOSECTION, 'controller_web_port'))
#  serv.start()
  serv.running = True
  serv.start_liveness_thread()
  serv.evtloop()  #call doesn't return

  
def get_server_on_this_node (endpoint_i = "", bind_port = DEFAULT_BIND_PORT):  
  endpoint = (endpoint_i, bind_port) #all interfaces?
  server = Controller(endpoint)
  return server


class Controller (ControllerAPI, JSServer):
  """A JetStream controller"""
  
  def __init__ (self, addr, hbInterval=CWorker.DEFAULT_HB_INTERVAL_SECS):
    JSServer.__init__(self, addr)
    self.workers = {}  # maps workerID = (hostid, port) -> CWorker. host and port are those visible HERE, NOT the nodeID
    self.nodeID_to_local = {}
    self.computations = {}  #maps ID to Computation
    self.cube_locations = {} #maps cube name to node ID. Only listed after cube create is
      # acknowledged. Note this is NODE ID, not local port!
    self.pending_work = {} #maps nodeID to an assignment.
    self.hbInterval = hbInterval
    self.running = False
    self.livenessThread = None
    
    # Given GIL, coarse-grained locking should be sufficient
    self.stateLock = threading.RLock()
    self.nextCompID = 1
    self.last_HB_ts = 0 # we only print one HB per second

  def handle_connection_close (self, cHandler):
    """Overrides parent class method."""
    wID = cHandler.cli_addr
#    logger.info("Marking worker %s:%d as dead due to closed connection" % (wID[0],wID[1]))  #Note not all sockets are with workers. There's also the client.
    with self.stateLock:
      self.worker_died(wID)
      JSServer.handle_connection_close(self, cHandler)
    
  
  def start (self):         #Note that start() returns after starting; it's useful for
    self.running = True     # embedding, not so much for standalone
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


  ### Accessor methods, callable externally

  def get_nodes (self):
    """Returns a list of Workers."""
    res = []
    self.stateLock.acquire()
    res.extend(self.workers.values())
    self.stateLock.release()
    return res

  #### This is dead code   
#  def get_one_node (self):
#    res = self.workers.keys()[0]
#    return res

  def get_cubes(self):
    """Returns a list of cube-name, cube-location pairs. For pending cubes, location is
     None."""
    res = []
    self.stateLock.acquire()
    res.extend(self.cube_locations.items())
    self.stateLock.release()
    return res

##########################################################################################
#
#  Everything below here is internal and should not be called externally
#
##########################################################################################



  ### Methods that don't touch state or that are thread safe


  def liveness_thread (self):
    while self.running:
      with self.stateLock:
        for wID,s in self.workers.items():
          # TODO: Just delete the node for now, but going forward we'll have to 
          # reschedule computations etc.
          if s.update_state() == CWorker.DEAD:
            logger.info("Marking worker %s:%d as dead due to timeout" % (wID[0],wID[1]))
            # This is thread-safe since we are using items() and not an iterator
            self.worker_died(wID)
      # All workers reporting to a controller should have the same hb interval
      # (enforced via common config file)
      time.sleep(self.hbInterval)



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

  def start_computation_async(self, assignments):
    t = threading.Thread(group=None, target=self.start_computation_sync, args=(assignments,))
    t.daemon = True
    t.start()    


  ### Methods that are not thread safe
  
  def start_computation_sync(self, assignments):
    for workerID,assignment in assignments.items():
      req = assignment.get_pb()            
      h = self.connect_to(workerID)
      #print worker, req
      # Send without waiting for response; we'll hear back in the main network message
      # handler
#      logger.info("XXX sending to %s", str(workerID))
      h.send_pb(req)
#      logger.info("XXX send to %s returned", str(workerID))

  def worker_died (self, workerConnID):
    """Called when a worker stops heartbeating and should be treated as dead.
    Manipulates the worker list (caller must ensure thread-safety)."""

    if workerConnID in self.workers.keys():
      worker_assignments = self.workers[workerConnID]
      nodeID = worker_assignments.get_dataplane_ep()
      if nodeID in self.nodeID_to_local:
        del self.nodeID_to_local[nodeID]
#      if len(worker_assignment.operators) + len(worker_assignment.cubes) > 0:
      logger.info("Saving pending work for disconnected node %s:%d" % nodeID)
      logger.info("assignment is " + str(worker_assignments))
      self.pending_work[nodeID] = worker_assignments
      for c in worker_assignments.get_all_cubes():
        self.cube_locations[c.name] = None #cube no longer visible
      del self.workers[workerConnID]

    #TODO: Reschedule worker's assignments elsewhere, etc.

  
  
  def handle_heartbeat (self, hb, clientEndpoint):
    t = long(time.time())
    response = None #but might be non-none if there's a reconnect
    with self.stateLock:
      if clientEndpoint not in self.workers:
        logger.info("Added worker %s; dp addr %s:%d" % 
            (str(clientEndpoint), hb.dataplane_addr.address, hb.dataplane_addr.portno))
        self.workers[clientEndpoint] = CWorker(clientEndpoint, self.hbInterval)
      node_count = len(self.workers)
      self.workers[clientEndpoint].receive_hb(hb)
      
      id_as_tuple = (hb.dataplane_addr.address, hb.dataplane_addr.portno)

      self.nodeID_to_local[id_as_tuple] = clientEndpoint
      
      if id_as_tuple in self.pending_work:
        prevAssignments = self.pending_work[id_as_tuple]
        if len(prevAssignments.assignments) > 0:
          response = ControlMessage()
          response.type = ControlMessage.ALTER
          for a in prevAssignments.assignments.values():
            a.fillin_alter(response.alter.add())
            self.workers[clientEndpoint].add_assignment(a)

      if t > self.last_HB_ts:
        logger.info("got heartbeat from sender %s. %d nodes in system" % ( str(clientEndpoint), node_count))
        self.last_HB_ts = t
    return response


  def handle_alter (self, response, altertopo):
    #TODO This method isn't quite thread safe, and should be.
    response.type = ControlMessage.OK
    
    if len(self.workers) == 0:
      errorMsg = "No workers available to deploy the topology"
      logger.warning(errorMsg)
      response.type = ControlMessage.ERROR
      response.error_msg.msg = errorMsg
      return # Note that we modify response in-place. (ASR: FIXME; why do it this way?)

    compID,comp = self.assign_comp_id()
    response.started_comp_id = compID
    
    workerLocations = dict([ (wID, w.get_dataplane_ep() ) for (wID, w) in self.workers.items() ])
    planner = QueryPlanner(workerLocations)  # these should be the dataplane addresses
    err = planner.take_raw_topo(altertopo)
    try:
      assignments = planner.get_assignments(compID)
    except SchemaError as e:
      err = str(e)
      
    if len(err) > 0:
      logger.warning("Invalid topology: %s",err)
      response.type = ControlMessage.ERROR
      response.error_msg.msg = err
      return
      
    with self.stateLock:  
      # Finalize the worker assignments
      # Should this be AFTER we hear back from workers?
      for workerID,assignment in assignments.items():
        comp.assign_worker(workerLocations[workerID], assignment)
        self.workers[workerID].add_assignment(assignment)
      logger.info("Starting computation %d with %d worker assignments" % (compID, len(assignments)))
        
    self.start_computation_async(assignments)
    return    #response is built up in an argument, rather than returned


  def handle_alter_response (self, alter_response, workerEndpoint):
    with self.stateLock:
      compID = alter_response.computationID
    
      for name in alter_response.cubesToStop:
        del self.cube_locations[name]

      if compID not in self.computations:
        #there's a race here if the job is being shut down and this is the death notice
        if len(alter_response.toStart) > 0:
          logger.warning("Invalid computation id %d in ALTER_RESPONSE message reporting operator starts" % (compID))
        # if a dead job and it's all stops, we ignore it quietly
      else:
      # Let the computation know which parts of the assignment were started/created
        actualAssignment = WorkerAssignment(alter_response.computationID, alter_response.toStart, alter_response.toCreate)
        nodeID = self.workers[workerEndpoint].get_dataplane_ep()
        self.computations[compID].update_worker(nodeID, actualAssignment)
        for cubeMeta in alter_response.toCreate:
          self.cube_locations[cubeMeta.name] = nodeID 
        if nodeID in self.pending_work:
          self.pending_work[nodeID].pruneStarted(alter_response)
      

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
      if len(req.alter) != 1:
        response.error_msg.msg = "One alter per request from client"
      else:
        self.handle_alter(response, req.alter[0])

    elif req.type == ControlMessage.ALTER_RESPONSE:
      for a in req.alter:
        self.handle_alter_response(a, handler.cli_addr)
      return  # no response

    elif req.type == ControlMessage.HEARTBEAT:
      response = self.handle_heartbeat(req.heartbeat, handler.cli_addr)
      if not response:
        return # no response
      
    elif req.type == ControlMessage.STOP_COMPUTATION:
      self.stop_computation(response, req)
    
    elif req.type == ControlMessage.OK:
      # This should not be used
      logger.fatal("Received dangling OK message from %s" % (str(handler.cli_addr)))
      assert(false)
      return  # no response
    elif req.type == ControlMessage.ERROR:
      logger.error("Incoming error from %s: %s" % ( str(handler.cli_addr), req.error_msg.msg))
      return # no response
    else:
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "Got unexpected control message"
      logger.error("Got unexpected control message %s" % str(req))

    #if we didn't return early, send the response
    handler.send_pb(response)


  def stop_computation(self, response, req):
    """NOT thread safe"""
    comp_to_stop = req.comp_to_stop
    logger.info("Stopping computation %d" % comp_to_stop)
    if comp_to_stop not in self.computations:
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "No such computation %d" % comp_to_stop
      return
      
      
    print self.computations[comp_to_stop].workerAssignments
    for nodeID in self.computations[comp_to_stop].workers_in_use():
      if nodeID in self.nodeID_to_local:
        worker = self.nodeID_to_local[nodeID]
        h = self.connect_to(worker)
        h.send_pb(req)         #we can re-use the existing stop message
        self.workers[worker].cleanup_computation(comp_to_stop)
      else:
        print "can't stop computation %d on %s:%d: not connected." % (comp_to_stop, nodeID[0], nodeID[1])
        
    #TODO    what about pending operators?
        
    response.type = ControlMessage.OK
    del self.computations[comp_to_stop]
    #response value is passed by reference, not returned
    return   
    


  def assign_comp_id(self):
    with self.stateLock:  
      compID = self.nextCompID
      self.nextCompID += 1
  
      comp = Computation(compID)
      self.computations[compID] = comp

    return compID,comp


if __name__ == '__main__':
  main()
  sys.exit(0)
