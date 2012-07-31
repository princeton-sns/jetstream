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
from jetstream_controlplane_pb2 import *
from server import ServerAPI
from generic_netinterface import JSServer


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  serv = get_server_on_this_node()
  serv.evtloop()

  
def get_server_on_this_node():  
  bind_port = DEFAULT_BIND_PORT
  address = ('localhost', bind_port) 
  server = CoordinatorServer(address)
  return server


class CoordinatorServer(ServerAPI, JSServer):
  
  def __init__(self, addr):
    JSServer.__init__(self, addr)
    self.nodelist = {}
    
  def get_nodes(self):
    """Serialize node list as list of protobuf NodeIDs"""
    res = []
    for node in self.nodelist.keys():
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
    self.nodelist[handler.cli_addr] = t  #TODO more meta here
    
  def process_message(self, buf, handler):
  
    req = ServerRequest()
    req.ParseFromString(buf)
#    print ("server got %d bytes," % len(buf)), req
    response = ServerResponse()
    
      #always send node count so length is never zero
    response.count_nodes = len(self.get_nodes()) 
    if req.type == ServerRequest.GET_NODES:
      node_list = self.get_nodes()
      response.nodes.extend(node_list)
      print "server responding to get_nodes with list of length %d" % len(node_list)
    #elif req.type == ServerRequest.DEPLOY:
    #  deploy(req.alter)
    elif req.type == ServerRequest.HEARTBEAT:
      self.handle_heartbeat(req, handler)
      return #without sending response to heartbeat
    handler.send_pb(response)

if __name__ == '__main__':
  main()
  sys.exit(0)