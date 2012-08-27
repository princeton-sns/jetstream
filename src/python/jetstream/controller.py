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
  address = ('localhost', bind_port) 
  server = Controller(address)
  return server


class Controller(ControllerAPI, JSServer):
  
  def __init__(self, addr):
    JSServer.__init__(self, addr)
    self.nodelist = {}
    self.internals_lock = threading.RLock()
    
    #SS: How do we feel about namedtuples?
    # Values in nodelist will be of type NodeInfo
    self.NodeInfo = namedtuple('NodeInfo', 'lastHeard')


######## The methods below are part of the 'internal' interface. 
#  They should acquire locks before accessing data structures


  def get_nodes(self):
    """Returns a list of NodeInfos"""
    self.internals_lock.acquire()
    res = []
    res.extend(self.nodelist.items())
    self.internals_lock.release()
    return res
    
  def get_one_node(self):
    self.internals_lock.acquire()
    res = self.nodelist.keys()[0]
    self.internals_lock.release()
    return res

    
#########  The methods below are part of the external interface.  
#  They should go through the 'internal' interface OR acquire locks
    
    
  def serialize_nodelist(self, nodes):
    """Serialize node list as list of protobuf NodeIDs"""
    res = []
    for node,_ in nodes:
      nID = NodeID()
      nID.address,nID.portno =  node
      res.append(nID)
    return res
 
    
  def handle_heartbeat(self, hbeat, handler):
    t = long(time.time())
    print "got heartbeat at %s." % time.ctime(t)
    print "sender was " + str(handler.cli_addr)
    print hbeat
    print ""
    self.internals_lock.acquire()
    self.nodelist[handler.cli_addr] = self.NodeInfo(t)  #TODO more meta here
    self.internals_lock.release()

    
  def handle_deploy(self, altertopo):
    if len(self.nodelist) == 0:
      print "WARNING: Worker node list on controller is empty!!"
      #TODO: Return some error message here. Are we using ServerResponse.error for this?
      return

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
  
    req = ControlMessage()
    req.ParseFromString(buf)
#    print ("server got %d bytes," % len(buf)), req
    response = ControlMessage()
    
    # Always send node count so length is never zero
    node_list = self.get_nodes()
    response.node_count = len(node_list) 
    
    if req.type == ControlMessage.GET_NODE_LIST_REQ:
      response.nodes.extend(self.serialize_nodelist(node_list))
      response.type = ControlMessage.NODES_RESPONSE
      print "server responding to get_nodes with list of length %d" % len(node_list)
    elif req.type == ControlMessage.ALTER:
      self.handle_deploy(req.alter)
      response.type = ControlMessage.NODES_RESPONSE  #FIXME should have something else here      
    elif req.type == ControlMessage.HEARTBEAT:
      self.handle_heartbeat(req, handler)
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
