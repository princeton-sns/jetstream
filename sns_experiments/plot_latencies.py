from collections import defaultdict
import re
import sys
import numpy
import numpy.linalg
from numpy import array



OUT_TO_FILE = True

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True


def main():
  infile = sys.argv[1]
  
  data = parse_infile(infile)
#  print data

  plot_overall_latencies(data)


def parse_infile(infile):
  f = open(infile, 'r')
  ret = defaultdict( dict ) # label --> bucket --> count
  for ln in f:
#    print ln
    _,ln = ln.split(':')  #ditch operator ID from echo output
    hostname, label, bucket, count = ln[2:-2].split(",")
#    if 'after' in label:
#      continue
    bucket = int(bucket)
    count = int(count)
    if bucket in ret[label]:    
      ret[label][bucket] += count
    else:
      ret[label][bucket] = count
    
  f.close()
  return ret

def plot_overall_latencies(data):
  
  latency_to_count_before = defaultdict(int)
  latency_to_count_after = defaultdict(int)

  for label,map in data.items():
    for bucket,count in map.items():
      if 'before' in label:
        latency_to_count_before[bucket] += count
      else:
        latency_to_count_after[bucket] += count

  before_total = sum(latency_to_count_before.values())
  after_total = sum(latency_to_count_after.values())
  print "Total of %d tuples before DB and %d after" %  (before_total,after_total )
  
  before_vals = [100.0 * x /before_total for x in  latency_to_count_before.values()]
  after_vals = [100.0 * x /after_total for x in  latency_to_count_after.values() ]
  MAX_Y = int(max ( max(before_vals), max(after_vals)))
  MAX_X = max( latency_to_count_after.keys())
  
  fig = plt.figure(figsize=(9,5))
  ax = fig.add_subplot(111)

  width = 0.85
  fig.subplots_adjust(bottom=0.2)
  plt.ylim( (0, 1.2 *  MAX_Y) )    
  
  before_bars = ax.bar(latency_to_count_before.keys(), before_vals, width, color='r')    
  
  bar_positions = [width + x for x in latency_to_count_after.keys()]
  after_bars = ax.bar(bar_positions, after_vals, width, color='y')    
  
  ax.legend( (before_bars[0], after_bars[0]), ('Before DB', 'After DB') )
  plt.ylabel('Fraction of Tuples', fontsize=24)
  plt.xlabel('Latency (MS)', fontsize=24)

#  plt.xticks(xlocations, justify(top_langs), fontsize = 18, rotation = 60)

  if OUT_TO_FILE:
      plt.savefig("latency_distrib.pdf")
      plt.close(fig)  
  

if __name__ == '__main__':
  main()