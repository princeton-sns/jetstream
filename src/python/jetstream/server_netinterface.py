import asyncore
import asynchat

import logging
import socket
import struct
import subprocess
import threading
import time

from jetstream_types_pb2 import *
from jetstream_controlplane_pb2 import *
from server import Server
from generic_netinterface import JSServer


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  get_server_on_this_node()
  asyncore.loop()

  
def get_server_on_this_node():  
  bind_port = DEFAULT_BIND_PORT
  address = ('localhost', bind_port) 
  server = CoordinatorServer(address)
  return server


class CoordinatorServer(Server, JSServer):
  
  def __init__(self, addr):
    JSServer.__init__(self, addr)
    
  def get_nodes(self):
    return []

  def process_message(self, buf, handler):
  
    req = ServerRequest()
    req.ParseFromString(buf)
    print req
    response = ServerResponse()
    response.count_nodes = len(self.get_nodes())
    if req.type == ServerRequest.GET_NODES:
      node_list = self.get_nodes()
      response.nodes.extend(node_list)
    #elif req.type == ServerRequest.DEPLOY:
    #  self.coordinator.deploy(req.alter)
    handler.send_pb(response)

if __name__ == '__main__':
  main()
  sys.exit(0)
