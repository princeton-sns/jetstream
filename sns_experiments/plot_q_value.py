from collections import defaultdict
import re
import sys
import numpy
import numpy.linalg
from numpy import array
from math import sqrt



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
  data = parse_data(infile)
  plot_error(data)
  

target_quant = "0.95"  
  
METHODS = ["Sketch","Sample","Histogram"]
def parse_data(infile):
  print "parsing..."
#returns a map from distribution to
# a map from summary type to a list of (size, accuracy) pairs
  f = open(infile, 'r')
  ret = defaultdict( dict ) # distribution --> summary_type --> size
#      --> list of error terms
  
#  best_q = {'sketch':float('inf'), 'sample':float('inf'), 'histogram':float('inf')}
  worst_q = dict( [ (x,0) for x in METHODS])
  
  for ln in f:
    if ln.startswith("DATA:"):
      _,dist,_,_,sz = ln.split(" ")
      sz = int ( sz)
    elif ln.startswith("rel err for "):  
      parts = ln.split(" ")
      est, q, rel_err  = parts[3], parts[5], float(parts[6])
      
      if q == target_quant:
        if not est in ret[dist]:
          ret[dist][est] = {}
        dist_for_method = ret[dist][est]
        if sz in dist_for_method:
          dist_for_method[sz].append( rel_err )
        else:
           dist_for_method[sz] = [rel_err ]
#       if err[m] / trueval > worst_q[m]:
#         worst_q[m] = err[m]
    
  f.close()
  return ret

symbols = ['ro-', 'b.-', 'g+--']

def meanstdv(x):
    n, mean, std = len(x), 0.0, 0.0
    for a in x:
      mean = mean + a
    mean = mean / float(n)
    for a in x:
      std = std + (a - mean)**2
    std = sqrt(std / float(n-1))
    return mean, std

def plot_error(all_data):
  for distrib_name, data in all_data.items():
    distrib_name = distrib_name.replace("_", " ")
    fig = plt.figure(figsize=(6,4))
    fig.subplots_adjust(bottom=0.2)

    
    print "plotting %s..." % distrib_name
    ax = fig.add_subplot(111)
    ax.set_yscale('symlog', linthreshy= 1e-8, linscaley = 1)
 
    ax.set_title('Accuracy finding %dth percentile of %s' % (int(100 * float(target_quant)), distrib_name))

    y_vals_for = {}
    y_errbars_for = {}
    maxy = 0
    for summary_name, size_to_errs in data.items():
      x_vals = size_to_errs.keys()
      x_vals.sort()
      y_vals_for[summary_name] = {}
      y_errbars_for[summary_name] = {}
      for size,y in sorted(size_to_errs.items()):  
        m,s =  meanstdv(y)
        y_vals_for[summary_name][size] = m
#        y_errbars_for[summary_name][x] = s
        adj_min = max( min(y), m/2)
        y_errbars_for[summary_name][size] = (m - adj_min, max(y) - m)

      maxy = max(maxy, max(y_vals_for[summary_name].values()))

    plt.axis([0, max(x_vals) + 10, 0, 2 * maxy])
    plt.ylabel("Relative Error", fontsize=18) 
    plt.xlabel("Summary size kb", fontsize=18)   

    for (summary_name,y_vals_map),symb in zip(y_vals_for.items(), symbols):
      y_vals = [y_vals_map[x] for x in x_vals]
      y_errs = [ [y_errbars_for[summary_name][x][min_or_max] for x in x_vals] for min_or_max in [0,1]] # for min/max errs
#      y_errs = [y_errbars_for[summary_name][x] for x in x_vals]
      print summary_name,y_vals
#      plt.plot(x_vals, y_vals, symb, label=summary_name)
      plt.errorbar(x_vals, y_vals,  fmt=symb ,yerr=y_errs , label=summary_name)
 
      # can do plt.
    ax.legend()
    if OUT_TO_FILE:
      plt.savefig(distrib_name +"_accuracy.pdf")
      plt.close(fig)  
  
  return


if __name__ == '__main__':
  main()
