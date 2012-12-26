from optparse import OptionParser 

from remote_controller import normalize_controller_addr

def arg_config():
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                    help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                    help="controller address", default="localhost:3456")

  (options, args) = parser.parse_args()
  serv_addr,serv_port = normalize_controller_addr(options.controller)
  file_to_parse = args[0]

  return ((serv_addr, serv_port), file_to_parse)
