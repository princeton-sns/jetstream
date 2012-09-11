import asyncore
import asynchat

import logging
import socket
import struct
import subprocess
import sys
import threading
import time
from collections import namedtuple

from jetstream_types_pb2 import *
from controller_api import ControllerAPI
from computation_state import *
from generic_netinterface import JSServer
from server_http_interface import start_web_interface


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  serv = get_server_on_this_node()
  start_web_interface(serv)
  serv.evtloop()

  
def get_server_on_this_node ():  
  bind_port = DEFAULT_BIND_PORT
  endpoint = (socket.gethostname(), bind_port) 
  server = Controller(endpoint)
  return server


class Controller (ControllerAPI, JSServer):
  """A JetStream controller"""
  
  def __init__ (self, addr, hbInterval=CWorker.DEFAULT_HB_INTERVAL_SECS):
    JSServer.__init__(self, addr)
    self.workers = {}
    self.computations = {}
    self.livenessThread = None
    self.hbInterval = hbInterval
    self.internals_lock = threading.RLock()


  def start_liveness_thread (self):
    assert(self.livenessThread == None)
    self.livenessThread = threading.Thread(group=None, target=self.liveness_thread, args=())
    self.livenessThread.daemon = True
    self.livenessThread.start()


  def liveness_thread (self):
    while len(self.workers) > 0:
      for w,s in self.workers.items():
        # Just delete the node for now, but going forward we'll have to reschedule
        # computations etc.
        if s.update_state() == CWorker.DEAD:
          logger.info("marking worker %s:%d as dead due to timeout"% (w[0],w[1])
          del self.workers[w]
      #TODO: Do all workers have the same hb interval?
      time.sleep(self.hbInterval)


  ###TODO: Decide whether we need locks for internal datastructure access in methods below.
    
  def get_nodes (self):
    """Returns a list of Workers"""
    self.internals_lock.acquire()
    res = []
    res.extend(self.workers.values())
    self.internals_lock.release()
    return res

    
  def get_one_node (self):
    self.internals_lock.acquire()
    res = self.workers.keys()[0]
    self.internals_lock.release()
    return res

  ###TODO: See above


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
    print "server responding to get_nodes with list of length %d" % len(nodeList)

    
  def handle_heartbeat (self, hb, clientEndpoint):
    t = long(time.time())
    print "got heartbeat at %s from sender %s" % (time.ctime(t), str(clientEndpoint))
    #print hb
    #print ""
    self.internals_lock.acquire()
    #TODO: Either remove locking above or add add() API
    if clientEndpoint not in self.workers:
      print "Added " + str(clientEndpoint)
      self.workers[clientEndpoint] = CWorker(clientEndpoint, self.hbInterval)
      self.workers[clientEndpoint].receive_hb(hb)
      # If this is the first worker, start the liveness thread
      if len(self.workers) == 1:
        # Make sure a previous instantiation of the thread has stopped
        if (self.livenessThread != None) and (self.livenessThread.is_alive()):
          self.livenessThread.join()
        self.livenessThread = None
        self.start_liveness_thread()
    else:
      self.workers[clientEndpoint].receive_hb(hb)
    self.internals_lock.release()

    
  def handle_alter (self, response, altertopo):
    response.type = ControlMessage.OK
    
    if len(self.workers) == 0:
      print "WARNING: Worker node list on controller is empty!!"
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "No workers available to deploy topology"
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

    for endpoint,assignment in assignments.items():
      comp.assign_worker(endpoint, assignment)
      
    # Start the computation
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
      print "Received OK response from " + str(handler.cli_addr)
      return  # no response
    else:
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "unknown error"

    handler.send_pb(response)


if __name__ == '__main__':
  main()
  sys.exit(0)
