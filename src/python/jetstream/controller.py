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

  
def get_server_on_this_node():  
  bind_port = DEFAULT_BIND_PORT
  endpoint = ('localhost', bind_port) 
  server = Controller(endpoint)
  return server


class Controller(ControllerAPI, JSServer):
  """A JetStream controller"""
  
  def __init__(self, addr, hbInterval=WorkerState.DEFAULT_HB_INTERVAL_SECS):
    JSServer.__init__(self, addr)
    self.workerList = {}
    self.livenessThread = None
    self.hbInterval = hbInterval
    self.internals_lock = threading.RLock()


  def start_liveness_thread(self):
    assert(self.livenessThread == None)
    self.livenessThread = threading.Thread(group=None, target=self.liveness_thread, args = ())
    self.livenessThread.daemon = True
    self.livenessThread.start()


  def liveness_thread(self):
    while len(self.workerList) > 0:
      for w,s in self.workerList.items():
        if s.update_state() == WorkerState.DEAD:
          del self.workerList[w]
      #TODO: Do all workers have the same hb interval?
      time.sleep(self.hbInterval)


  ###TODO: Decide whether we need locks for internal datastructure access in methods below.
    
  def get_nodes(self):
    """Returns a list of WorkerStates"""
    self.internals_lock.acquire()
    res = []
    res.extend(self.workerList.values())
    self.internals_lock.release()
    return res

    
  def get_one_node(self):
    self.internals_lock.acquire()
    res = self.workerList.keys()[0]
    self.internals_lock.release()
    return res

  ###TODO: See above


  def serialize_nodeList(self, nodes):
    """Serialize node list as list of protobuf NodeIDs"""
    res = []
    for node in nodes:
      nID = NodeID()
      nID.address,nID.portno = node.endpoint
      res.append(nID)
    return res


  def handle_get_nodes(self, response):
    nodeList = self.get_nodes()
    response.type = ControlMessage.NODES_RESPONSE
    response.nodes.extend(self.serialize_nodeList(nodeList))
    response.node_count = len(nodeList)
    print "server responding to get_nodes with list of length %d" % len(nodeList)

    
  def handle_heartbeat(self, hb, clientAddr):
    t = long(time.time())
    print "got heartbeat at %s from sender %s" % (time.ctime(t), str(clientAddr))
    #print hb
    #print ""
    self.internals_lock.acquire()
    #TODO: Either remove locking above or add add() API
    if clientAddr not in self.workerList:
      print "Added " + str(clientAddr)
      self.workerList[clientAddr] = WorkerState(clientAddr, self.hbInterval)
      self.workerList[clientAddr].receive_hb(hb)
      # If this is the first worker, start the liveness thread
      if len(self.workerList) == 1:
        # Make sure a previous instantiation of the thread has stopped
        if (self.livenessThread != None) and (self.livenessThread.is_alive()):
          self.livenessThread.join()
        self.livenessThread = None
        self.start_liveness_thread()
    else:
      self.workerList[clientAddr].receive_hb(hb)
    self.internals_lock.release()

    
  def handle_deploy(self, response, altertopo):
    response.type = ControlMessage.OK
    
    if len(self.workerList) == 0:
      print "WARNING: Worker node list on controller is empty!!"
      response.type = ControlMessage
      response.error_msg.msg = "No workers available to deploy topology"

    #TODO: The code below only deals with starting tasks. We also assume the client topology looks
    #like an in-tree from the stream sources to a global union point, followed by an arbitrary graph.

    # Find the first global union point, or the LCA of all sources.
    taskID = self.findSourcesLCA(altertopo.toStart, altertopo.edges)

    # Assign pinned tasks to specified workers. For now, assign unpinned tasks to a default worker.
    workerToTasks = {}
    defaultWorker = self.get_one_node()
    for task in altertopo.toStart:
      workerAddr = defaultWorker
      if task.site.address != '':
        # Task is pinned, so overwrite the target worker address
        workerAddr = (task.site.address, task.site.portno)
      if workerAddr in workerToTasks:
        workerToTasks[workerAddr].append(task)
      else:
        workerToTasks[workerAddr] = [task]

    # Deploy the tasks assigned to each worker
    for workerAddr in workerToTasks.keys():
      req = ControlMessage()
      req.type = ControlMessage.ALTER
      req.alter.toStart.extend(workerToTasks[workerAddr])
      h = self.connect_to(workerAddr)
      h.send_pb(req)

    print "returning from handle_deploy"


  #TODO: Move this to within operator graph abstraction.
  def findSourcesLCA(self, tasks, edges):
    #TODO: For now just assert that the sources point to the same parent and return this parent
    #taskIdToTask = {}
    #for task in tasks:
    return -1


  def process_message(self, buf, handler):
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
      self.handle_deploy(response, req.alter)
    elif req.type == ControlMessage.HEARTBEAT:
      self.handle_heartbeat(req, handler.cli_addr)
      return # no response
    elif req.type == ControlMessage.OK:
      print "Received OK response from " + str(handler.cli_addr)
      return # no response
    else:
      response.type = ControlMessage.ERROR
      response.error_msg.msg = "unknown error"

    handler.send_pb(response)


if __name__ == '__main__':
  main()
  sys.exit(0)
