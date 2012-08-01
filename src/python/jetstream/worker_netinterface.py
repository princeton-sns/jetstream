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
from jetstream_controlplane_pb2 import *
from worker_api import WorkerAPI
from generic_netinterface import JSServer


logger = logging.getLogger('JetStream')
DEFAULT_BIND_PORT = 3456
def main():
#  Could read config here
  worker = get_worker_on_this_node()
  worker.evtloop()

  
def get_worker_on_this_node():  
  bind_port = DEFAULT_BIND_PORT
  address = ('localhost', bind_port) 
  worker = WorkerServer(address)
  return worker

class WorkerServer(WorkerAPI, JSServer):
  
  #def __init__(self, addr):
  #  JSServer.__init__(self, addr)

  def handle_deploy(self, altertopo):
    print "GOT WORKER DEPLOY!"

  def process_message(self, buf, handler):
  
    req = WorkerRequest()
    req.ParseFromString(buf)
    response = WorkerResponse()
    # Always send some message so length is not 0
    response.error = "No error"
    if req.type == WorkerRequest.DEPLOY:
      self.handle_deploy(req.alter)
    handler.send_pb(response)

if __name__ == '__main__':
  main()
  sys.exit(0)
