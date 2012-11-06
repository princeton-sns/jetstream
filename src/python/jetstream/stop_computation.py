from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import sys
import time

from remote_controller import RemoteController, normalize_controller_addr

def main():

  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")

  (options, args) = parser.parse_args()
  
  if len(args) < 1:
    print "usage:  stop_computation [computation ID]"
    sys.exit(0)
  
  serv_addr, serv_port = normalize_controller_addr(options.controller)

    #Unlike most Jetstream programs, need to know how many nodes we have to set up the distribution properly
  print "connecting..."
  server = RemoteController()
  server.connect(serv_addr, serv_port)
  print "connected to remote, sending stop"
  resp = server.stop_computation (args[0])
  print "server response:", resp
  
  

if __name__ == '__main__':
    main()