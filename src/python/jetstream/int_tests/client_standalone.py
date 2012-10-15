import getopt
import os
import signal
import urllib2
import asyncore
import random
import socket
import struct
import thread
import time
import unittest

from controller import *
from generic_netinterface import JSClient

from jetstream_types_pb2 import *


##### DEFAULT PARAMETERS ###
DEFAULT_CONTROLLER_EP = ('localhost', 3456)
DEFAULT_WEB_PORT = 8081

def usage():
  usageStr = "Usage: \n\n"
  usageStr = usageStr + "./client_standalone.py [-c controller-endpoint] [-w web-interface-port] [-h help]"
  return usageStr

def errorExit(error):
  print error + "\n"
  sys.exit(1)


def main(argv):
  # Parse the command-line arguments.
  try:
    opts, args = getopt.getopt(argv, "c:w:", [])
  except getopt.GetoptError:
    errorExit("Error parsing command line options\n" + usage())

  controllerEp = DEFAULT_CONTROLLER_EP
  webPort = DEFAULT_WEB_PORT
  for opt, arg in opts:
    if opt == "-h":
      print usage()
      sys.exit(0)
    elif opt == "-c":
      controllerEp = string.split(arg, ':')
      assert(len(controllerEp) == 2)
    elif opt == "-w":
      webPort = int(arg)
    else:
      errorExit("Unexpected command line argument " + opt + "\n" + usage())

  # Uncomment For local testing:
  controller = Controller(('localhost', 0))
  controller.start()
  print "controller bound to %s:%d" % controller.address

  # Create a worker and give it enough time to heartbeat (i.e. register with the controller)
  jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (controller.address[1])
  workerProc1 = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
  jsnode_cmd = "../../jsnoded -a localhost:%d --start -C ../../config/datanode.conf" % (controller.address[1])
  workerProc2 = subprocess.Popen(jsnode_cmd, shell=True, preexec_fn=os.setsid)
  time.sleep(2)

  controllerEp = controller.address

  # Create a JS client
  jsClient = JSClient(controllerEp)

  # Get the list of alive workers
  req = ControlMessage()
  req.type = ControlMessage.GET_NODE_LIST_REQ
  buf = jsClient.do_rpc(req, True)
  resp = ControlMessage()
  resp.ParseFromString(buf)
  workersEp = resp.nodes
  # This is a distributed test, so fail if there are fewer than 2 workers
  assert(len(workersEp) >= 2)
  
  # Issue a query that runs an operator on each worker: send some tuples from a
  # source, filter them through the remaining workers and collect at the end.
  req = ControlMessage()
  # Map from worker endpoint str -> deployed operators' names
  assignedOps = {}
  # Config parameters
  compID = 17
  kStr = "5"
  for i in range(len(workersEp)):
    if workersEp[i].address not in assignedOps:
      assignedOps[workersEp[i].address] = []
    req.type = ControlMessage.ALTER
    req.alter.computationID = compID
    task = req.alter.toStart.add()
    task.id.computationID = compID
    task.id.task = i + 1  # start task numbers at 1
    # Pin the operator to the current worker
    task.site.address = workersEp[i].address
    task.site.portno = workersEp[i].portno

    if i == 0:
      # Send some tuples from the first worker
      task.op_typename = "SendK"
      opCfg = task.config.add()
      opCfg.opt_name = "k"
      opCfg.val = kStr
    elif i == len(workersEp) - 1:
      # Collect tuples at the last worker
      task.op_typename = "DummyReceiver"
    else:
      # Insert no-op filters in between
      task.op_typename = "StringGrep"
      opCfg = task.config.add()
      opCfg.opt_name = "pattern"
      opCfg.val = ".*"
      opCfg2 = task.config.add()
      opCfg2.opt_name = "id"
      opCfg2.val = "0"

    assignedOps[workersEp[i].address].append(task.op_typename)

    if i > 0:
      # Create an edge from the previous operator to this one
      e = req.alter.edges.add()
      e.src = task.id.task - 1
      e.dest = task.id.task
      e.computation = compID
      # Provide remote destination info for the edge
      e.dest_addr.address = workersEp[i].address
      e.dest_addr.portno = 3456

  # Deploy the query
  buf = jsClient.do_rpc(req, True)
  resp = ControlMessage()
  resp.ParseFromString(buf)
  assert(resp.type == ControlMessage.OK)
  time.sleep(2)

  for i in range(len(workersEp)):
    # GET the web interface of each worker and make sure the right operators appear
    url = "http://" + workersEp[i].address + ":" + str(webPort) + "/"
    getResp = urllib2.urlopen(url).read()
    for opName in assignedOps[workersEp[i].address]:
      assert(opName in getResp)

  # Cleanup
  jsClient.close()
  # Uncomment for local testing:
  os.killpg(workerProc1.pid, signal.SIGTERM)
  os.killpg(workerProc2.pid, signal.SIGTERM)

  
if __name__ == '__main__':
  main(sys.argv[1:])
  sys.exit(0)
