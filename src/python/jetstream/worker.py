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
from jetstream_dataplane_pb2 import *


from generic_netinterface import JSServer
from server_netinterface import DEFAULT_BIND_PORT

logger = logging.getLogger('JetStream')
def main():
#  Could read config here
  
  server_address = ('localhost', DEFAULT_BIND_PORT)
  worker_thread = create_worker(server_address)
  print "connected, starting heartbeat thread" 
  worker_thread.heartbeat_thread()
 
def create_worker(server_address):
  my_address = ('localhost', 0) 
  cli_loop = WorkerAPIImpl(my_address)
  cli_loop.connect_to_server(server_address)
  cli_loop.start_as_thread()
  return cli_loop


class WorkerAPIImpl(JSServer):
  
  def __init__(self, addr):
    JSServer.__init__(self, addr)
    self.looping = True

  def connect_to_server(self, server_address):
    self.connection_to_server = self.connect_to(server_address)

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
    
    

  def start_heartbeat_thread(self):
    t = threading.Thread(group = None, target =self.heartbeat_thread, args = ())
    t.daemon = True
    t.start()

  def heartbeat_thread(self):
    req = ServerRequest()
    while self.looping:
      print "sending HB"
      req.type = ServerRequest.HEARTBEAT
      req.heartbeat.freemem_mb = 100
      req.heartbeat.cpuload_pct = 100 #TODO: find real values here
      self.connection_to_server.send_pb(req)
  
      time.sleep(5)
  

if __name__ == '__main__':
  main()
