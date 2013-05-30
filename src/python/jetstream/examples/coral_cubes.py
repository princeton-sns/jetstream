from collections import defaultdict
import csv
from optparse import OptionParser
import random
import sys
import time


from jetstream_types_pb2 import *
from remote_controller import *
import query_graph as jsapi
from query_planner import QueryPlanner

from coral_parse import coral_fnames,coral_fidxs, coral_types
from coral_util import *   #find_root_node, standard_option_parser,

def main():

  parser = standard_option_parser()
  parser.add_option("--full_url", dest="full_url", action="store_true", default=False)

  (options, args) = parser.parse_args()

  if not options.fname:
    print "you must specify the input file name [with -f]"
    sys.exit(1)

  all_nodes,server = get_all_nodes(options)
  root_node = find_root_node(options, all_nodes)
  source_nodes = get_source_nodes(options, all_nodes, root_node)
  
  for node in all_nodes:
    g = get_graph(node,  options)
    deploy_or_dummy(options, server, g)

def get_graph(node, options):
  g= jsapi.QueryGraph()

  start_ts = parse_ts(options.start_ts)

  parsed_field_offsets = [coral_fidxs['timestamp'], coral_fidxs['HTTP_stat'],\
     coral_fidxs['URL_requested'], coral_fidxs['nbytes'], coral_fidxs['dl_utime'], len(coral_types) ]


  f = jsapi.FileRead(g, options.fname, skip_empty=True)
  csvp = jsapi.CSVParse(g, coral_types)
  csvp.set_cfg("discard_off_size", "true")
  round = jsapi.TimeWarp(g, field=1, warp=options.warp_factor)
  round.set_cfg("wait_for_catch_up", "true")
  f.instantiate_on(node)
  
  local_raw_cube = g.add_cube("local_records")
  local_raw_cube.instantiate_on(node)
  local_raw_cube.set_overwrite(True)
  
  define_schema_for_raw_cube(local_raw_cube, parsed_field_offsets)
  if not options.full_url:
    url_to_dom = jsapi.URLToDomain(g, field=coral_fidxs['URL_requested'])
    g.chain( [f, csvp, round, url_to_dom, local_raw_cube] )
  else:
    g.chain( [f, csvp, round, local_raw_cube] )
  return g

if __name__ == '__main__':
    main()

