from jetstream_types_pb2 import *

from optparse import OptionParser 

import random
import socket
import time
import string
import re
from itertools import izip, tee
from operator import itemgetter

from remote_controller import RemoteController,normalize_controller_addr
import query_graph as jsapi
from client_reader import ClientDataReader

CORAL_N = 17

coral_types = "IDSSSSSSIIIIIIISS"
assert len(coral_types) == CORAL_N
coral_fnames = ("version", "timestamp", "IN_OUT", "from_cache", "client_ip",
                "server_ip", "URL_requested", "Referrer_URL", "HTTP_stat",
                "nbytes", "CORAL1", "CORAL2", "CORAL3", "CORAL4", "dl_utime",
                "X_Forwarded_For_header", "VIA_header")
coral_fidxs = {}
for index, field in enumerate(coral_fnames):
  coral_fidxs[field] = index
assert len(coral_fnames) == len(coral_types)

URL_FIELDS = set(coral_fidxs[field] for field in
                                   filter(lambda s: 'URL' in s, coral_fnames))

assert set([6, 7]) == URL_FIELDS

DELIMS = ",\"\s"
FLD_REGEX = "[^" + DELIMS + "]*"
FLD_CAPTURE = "(" + FLD_REGEX + ")"
# this is annoying, but URLs can have commas in them.
URL_CAPTURE = "([^\"\s]*)"
DOMAIN_CAPTURE = "http://([[:alnum:]\.]*)" + FLD_REGEX # gobble rest of URL
SEP_REGEX = "[" + DELIMS + "]*" 

CORAL_MATCH = tuple([FLD_REGEX] * CORAL_N)

# from python itertools recipes
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

def coral_partial_regex(idxs, want_domains=False):
  """ Field_indices should be the column numbers of the desired fields from a
  CoralCDN log, numbered starting from 0 """
  if any(0 > index >= CORAL_N for index in idxs):
    raise Exception('Bad coral index')

  # copy list of regexes, replacing specified fields with capture regexes,
  # before joining with the separating regex
  rgx_template = list(CORAL_MATCH)
  for index in idxs:
    if index in URL_FIELDS:
      if want_domains:
        rgx_template[index] = DOMAIN_CAPTURE
      else:
        rgx_template[index] = URL_CAPTURE
    else:
      rgx_template[index] = FLD_CAPTURE

  return SEP_REGEX + SEP_REGEX.join(rgx_template) + SEP_REGEX

# used to work before adding different different captures for URL fields
#assert coral_partial_regex(range(CORAL_N)) == (SEP_REGEX + \
                           #SEP_REGEX.join([FLD_CAPTURE] * CORAL_N) \
                           #+ SEP_REGEX)

print "passed silly assert"

def test_regex(rgx):
  re.compile(rgx)
test_on = [[1,5,7], [1,9], [7], [1], [0], [0, 11], [0, 16], range(1, 9),
          range(1, CORAL_N), range(CORAL_N), range(4)]
for test_case in test_on:
  test_regex(coral_partial_regex(test_case))


def main():
  parser = OptionParser()
  parser.add_option("-C", "--config", dest="config_file",
                  help="read config from FILE", metavar="FILE")

  parser.add_option("-a", "--controller", dest="controller",
                  help="controller address", default="localhost:3456")

  (options, args) = parser.parse_args()
  serv_addr,serv_port = normalize_controller_addr(options.controller)
  file_to_parse = args[0]
  
  k2 = 20 #how many to pull to top level
  k = 10 #how many to display
  
  # specify the query fields that this computation is interested in
  which_coral_fields = [coral_fidxs['URL_requested']]

  subfields = itemgetter(*which_coral_fields)
  which_types = "".join(subfields(coral_types))
  which_names = subfields(coral_fnames)
  regex = coral_partial_regex(which_coral_fields, want_domains=True)
  print "type parameter: ", which_types
  print "name parameter: ", which_names
  print "regex: ", regex

  server = RemoteController()
  server.connect(serv_addr, serv_port)
  n = server.get_a_node()
  assert isinstance(n, NodeID)
  all_nodes = server.all_nodes()

  g = jsapi.QueryGraph()
  f = jsapi.FileRead(g, file_to_parse, skip_empty=True)
  scanf = jsapi.GenericParse(g, regex, which_types)
  cr = ClientDataReader()
  pull_k2 = jsapi.TimeSubscriber(g, {}, 2000, "-count", k2)

  local_cube = g.add_cube("coral_results")
  add_cube_dims(local_cube, which_coral_fields)
  # index past end of tuple is a magic API to the "count" aggregate that tells
  # it to assume a count of 1
  local_cube.add_agg("count", jsapi.Cube.AggType.COUNT, len(which_types))
  local_cube.set_overwrite(True)  #fresh results

  g.connect(f, scanf)
  g.connect(scanf, local_cube)
  g.connect(local_cube, pull_k2)
  g.connectExternal(pull_k2, cr.prep_to_receive_data())
  server.deploy(g)

  tuples = []
  def processdata(tup):
    assert len(tup.e) == len(which_types) + 1
    tuples.append(tup)
    #print tup
    x = CoralLogLine(tup, which_names, which_types)
  cr.blocking_read(processdata)


class CoralLogLine():
  def __init__(self, coral_tuple, fields, types):
    self.fields = []
    for i, (field, typ) in enumerate(izip(fields, types)):
      assert field != "fields"

      if typ == "S":
        setattr(self, field, string.strip(coral_tuple.e[i].s_val, '"'))
      elif typ == "I":
        setattr(self, field, int(coral_tuple.e[i].i_val))
      elif typ == "D":
        setattr(self, field, float(coral_tuple.e[i].d_val))
      else:
        raise TypeError("Unxpected field type: %s" % str(typ))
      self.fields.append(getattr(self, field))

    print self.fields


# This method adds correctly typed fields to the cube, assuming that they are
# stored in the order provided in the argument
def add_cube_dims(cube, field_indices):
  specifier_to_type = {"I" : Element.INT32,  "S" : Element.STRING,
                       "D" : Element.DOUBLE, "T" : Element.TIME}
  subfields = itemgetter(*field_indices)
  names = subfields(coral_fnames)
  types = subfields(coral_types)

  for index, (name, kind) in enumerate(izip(names, types)):
    t = None
    try:
      t = specifier_to_type[kind]
    except KeyError:
      raise KeyError("Invalid cube format type: %s" % kind)
    cube.add_dim(name, t, index)


if __name__ == '__main__':
    main()
