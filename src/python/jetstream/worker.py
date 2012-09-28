#
# Minimal implementation of a JetStream worker used for testing only (actual
# worker implementation is in C++ and lives in jetstream/src/dataplane).
#

import asyncore
import asynchat

import logging
import socket
import struct
import threading
import time

from jetstream_types_pb2 import *
from local_controller import LocalUnix

from generic_netinterface import JSServer
from controller import DEFAULT_BIND_PORT
from computation_state import CWorker

logger = logging.getLogger('JetStream')
DEFAULT_WORKER_BIND_PORT = 0


def main():
  # Could read config here
  server_address = ('localhost', DEFAULT_BIND_PORT)
  worker_thread = create_worker(server_address)
  print "connected, starting heartbeat thread" 
  worker_thread.heartbeat_thread()

 
def create_worker(controllerAddr, hbInterval=CWorker.DEFAULT_HB_INTERVAL_SECS):
  myAddr = ('localhost', DEFAULT_WORKER_BIND_PORT) 
  newWorker = Worker(myAddr, controllerAddr, hbInterval)
  print "worker bound to " + str(newWorker.address)
  return newWorker


class Worker(JSServer):
  
  def __init__(self, addr, controllerAddr, hbInterval=CWorker.DEFAULT_HB_INTERVAL_SECS):
    JSServer.__init__(self, addr)
    self.tasks = {}
    self.controllerConn = self.connect_to(controllerAddr)
    self.hbInterval = hbInterval
    self.running = False
    self.hbThread = None


  def start(self):
    self.running = True
    JSServer.start(self)
    # Start the heartbeat thread
    self.start_heartbeat_thread()
    

  def stop(self):
    self.running = False
    self.stop_heartbeat_thread()
    JSServer.stop(self)


  def handle_deploy(self, altertopo):
    #TODO: Assumes operators are all command line execs
    for task in altertopo.toStart:
      if task.op_typename != "":
        #TODO: Why is TaskID unhashable?
        self.tasks[task.id.task] = LocalUnix(task.id.task, task.op_typename)
        self.tasks[task.id.task].start()
    for taskId in altertopo.taskToStop:
      if taskId.task in self.tasks:
        #TODO Add stop() method to operator interface and stop tasks here
        del self.tasks[taskId.task]


  def process_message(self, buf, handler):
  
    req = ControlMessage()
    req.ParseFromString(buf)
#    response = ControlMessage()
    if req.type == ControlMessage.ALTER:
      self.handle_deploy(req.alter)
#    handler.send_pb(response)

    
  def start_heartbeat_thread(self):
    self.running = True
    self.hbThread = threading.Thread(group=None, target=self.heartbeat_thread, args=())
    self.hbThread.daemon = True
    self.hbThread.start()


  def stop_heartbeat_thread(self):
    self.running = False
    if (self.hbThread != None) and (self.hbThread.is_alive()):
      self.hbThread.join()


  def heartbeat_thread(self):
    req = ControlMessage()
    while self.running:
      print "sending HB"
      req.type = ControlMessage.HEARTBEAT
      req.heartbeat.freemem_mb = 100
      req.heartbeat.cpuload_pct = 100 #TODO: find real values here
      self.controllerConn.send_pb(req)
      time.sleep(self.hbInterval)


if __name__ == '__main__':
  main()
