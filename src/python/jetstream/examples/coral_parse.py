from jetstream_types_pb2 import *

from itertools import izip, tee
from collections import deque

from remote_controller import RemoteController
import query_graph as jsapi
from client_reader import ClientDataReader, tuple_str

import js_client_config

CORAL_N = 17
coral_types = "IDSSSSSSIIIIIIISS"
assert len(coral_types) == CORAL_N

coral_fnames = ("version", "timestamp", "IN_OUT", "from_cache", "client_ip",
                "server_ip", "URL_requested", "Referrer_URL", "HTTP_stat",
                "nbytes", "CORAL1", "CORAL2", "CORAL3", "CORAL4", "dl_utime",
                "X_Forwarded_For_header", "VIA_header")
assert len(coral_fnames) == len(coral_types)

coral_fidxs = {}
for index, field in enumerate(coral_fnames):
  coral_fidxs[field] = index

URL_FNAMES = filter(lambda fn: 'URL' in fn, coral_fnames)
URL_FIELDS = set(coral_fidxs[field] for field in URL_FNAMES)
assert set([6, 7]) == URL_FIELDS

DELIMS = ",\"\s"
FLD_REGEX = "[^" + DELIMS + "]*"
FLD_CAPTURE = "(" + FLD_REGEX + ")"
# this is annoying, but URLs can have commas in them.
URL_CAPTURE = "([^\"\s]*)"
#DOMAIN_CAPTURE = "\s*http://([[:alnum:]\.-\\\(\\\)]*).*"# + FLD_REGEX # gobble rest of URL
DOMAIN_CAPTURE = "\s*http://(?:www\.)?([^\/]*).*"# + FLD_REGEX # gobble rest of URL
SEP_REGEX = "[" + DELIMS + "]*" 

CORAL_MATCH = tuple([FLD_REGEX] * CORAL_N)

# from python itertools recipes
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)


# start a connection to a remote controller, instantiate any cubes on all of
# its nodes, and deploy the graph
def remote_deploy(serv_addr, serv_port, graph, cube=None):
  server = RemoteController((serv_addr, serv_port))
  assert isinstance(server.get_a_node(), NodeID)

  server.deploy(graph, cube)


def parse_setup():
  (serv_addr, serv_port), file_to_parse = js_client_config.arg_config()

  k2 = 20 # how many to pull to top level
  k = 10 # how many to display

  # specify the query fields that this computation is interested in
  #which_coral_fields = [coral_fidxs['URL_requested']]
  agg_field_idx = coral_fidxs['URL_requested']

  g = jsapi.QueryGraph()
  f = jsapi.FileRead(g, file_to_parse, skip_empty=True)
  csvp = jsapi.CSVParse(g, coral_types)
  grab_domain = jsapi.GenericParse(g, DOMAIN_CAPTURE,
                                   coral_types[agg_field_idx],
                                   field_to_parse=agg_field_idx,
                                   keep_unparsed=False)
  pull_k2 = jsapi.TimeSubscriber(g, {}, 2000, "-count", k2)

  local_cube = g.add_cube("coral_results")
  local_cube.add_dim("Requested_domains", Element.STRING, 0)
  # index past end of tuple is a magic API to the "count" aggregate that tells
  # it to assume a count of 1
  local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, 1)
  local_cube.set_overwrite(True)  # fresh results

  g.chain([f, csvp, grab_domain, local_cube, pull_k2])

  cr = ClientDataReader(raw_data=True)
  g.connectExternal(pull_k2, cr.prep_to_receive_data())
  remote_deploy(serv_addr, serv_port, g, cube=local_cube)

  return cr


if __name__ == '__main__':
  import coral_analyzer
  coral_analyzer.main()
