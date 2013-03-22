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
    _, bucket = bucket.split("=");
    _, count = count.split("=")
#    label = " ".join(label.split(" ")[0:3])
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


def median(values, total):
  running_tally = 0
  for k,v in sorted(values.items()):
    running_tally += v
    if running_tally > total/2:
      return k
  return INFINITY

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
  print "Median before is %d ms" % median(latency_to_count_before, before_total)
  print "Median after is %d ms" % median(latency_to_count_after, after_total)

  
  before_vals = [100.0 * x /before_total for _,x in  sorted(latency_to_count_before.items())]
  after_vals = [100.0 * x /after_total for _,x in sorted(latency_to_count_after.items()) ]
  MAX_Y = int(max ( max(before_vals), max(after_vals)))
  MAX_X = max( latency_to_count_after.keys())
  MIN_X = min( 0,  min (latency_to_count_after.keys() ), min(latency_to_count_before.keys() ) ) 
  
  print "latencies range from %d to %d" % (MIN_X, MAX_X)
  
  fig = plt.figure(figsize=(9,5))
  ax = fig.add_subplot(111)

  width = (MAX_X+(-MIN_X))/100.0 /2
  fig.subplots_adjust(bottom=0.14)
  fig.subplots_adjust(left=0.1)

  plt.ylim( (0, 1.2 *  MAX_Y) )    
  plt.xlim( (-1, MAX_X * 1.2) )   # ( -MAX_X * 1.2, MAX_X * 1.2) 

  before_bars = ax.plot(sorted(latency_to_count_before.keys()), before_vals, 'r.-')      
  bar_positions = [x for x in sorted(latency_to_count_after.keys())]
  after_bars = ax.plot(bar_positions, after_vals, "k-")    
  
  ax.legend( (before_bars[0], after_bars[0]), ('Before DB', 'After DB') )
  plt.ylabel('Fraction of Tuples', fontsize=24)
  plt.xlabel('Latency (MS)', fontsize=24)
  plt.tick_params(axis='both', which='major', labelsize=16)

#  plt.xticks(xlocations, justify(top_langs), fontsize = 18, rotation = 60)

  if OUT_TO_FILE:
      plt.savefig("latency_distrib.pdf")
      plt.close(fig)  
  

if __name__ == '__main__':
  main()
