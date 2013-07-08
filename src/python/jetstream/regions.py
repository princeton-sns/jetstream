import re
import sys


def read_regions(fname):
  regions = {}
  srcFile = open(fname, 'r')
  for ln in srcFile:
    r_name, r_pat = ln.split(" ")
    r_pat = r_pat.strip()
    r_pat = r_pat.replace("*", "[0-9]+")  #dots are super common in IP addresses so we 
    r_pat = r_pat.replace(".", "\.")  #dots are super common in IP addresses so we 
          # escape them by default
    regions[r_name] = re.compile(r_pat)
  srcFile.close()
  return regions

def get_1_from_region(region_re, nodelist):
  for n in nodelist:
    if region_re.match(n.address):
      return n
      
      
def get_region(r_list, nodeID):
  node_addr = nodeID.address
  for rgn, region_re in r_list.items():
    print "trying to match %s against %s" % (node_addr, region_re.pattern)
    if region_re.match(node_addr):
      return rgn
  print "No region for address " + nodeID.address
  print "Regions:", r_list
