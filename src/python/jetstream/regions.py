

import re
import sys


def read_regions(fname):
  regions = {}
  srcFile = open(fname, 'r')
  for ln in srcFile:
    r_name, r_pat = ln.split(" ")
    regions[r_name] = re.compile(r_pat)
  srcFile.close()
  return regions

def get_1_from_region(region_re, nodelist):
  for n in nodelist:
    if region_re.match(n[0]):
      return n