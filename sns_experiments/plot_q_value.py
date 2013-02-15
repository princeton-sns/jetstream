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
  data = parse_data(infile)
  plot_error(data)
  

target_quant = "0.95"  
  
METHODS = ["Sketch","Sample","Histogram"]
def parse_data(infile):
  print "parsing..."
#returns a map from distribution to
# a map from summary type to a list of (size, accuracy) pairs
  f = open(infile, 'r')
  ret = defaultdict( dict ) # distribution --> summary_type --> (size, accuracy)
  
#  best_q = {'sketch':float('inf'), 'sample':float('inf'), 'histogram':float('inf')}
  worst_q = dict( [ (x,0) for x in METHODS])
  
  for ln in f:
    if ln.startswith("DATA:"):
      _,dist,_,_,sz = ln.split(" ")
      sz = int ( sz)
    elif ln.startswith("Quantile"):
      continue
    else:
      err = {}
      q,trueval, err['Sketch'], err['Sample'], err['Histogram'] = [float(x) for x in ln.split(",")]
      if q == float(target_quant):
        for m in METHODS:
          rel_err = err[m]/trueval  + 1e-6 #so log scale plot looks ok  
          if m in ret[dist]:
            ret[dist][m].append( (sz, rel_err) )
          else:
            ret[dist][m] = [ (sz, rel_err) ]
#       if err[m] / trueval > worst_q[m]:
#         worst_q[m] = err[m]
    
  f.close()
  return ret

symbols = ['ro-', 'bo-', 'go-']

def plot_error(all_data):
  for distrib_name, data in all_data.items():
    fig = plt.figure(figsize=(9,5))
    print "plotting %s..." % distrib_name
    ax = fig.add_subplot(111)
    ax.set_yscale('log')
    ax.set_title('Accuracy for ' + distrib_name +  " at " + target_quant)


    y_vals_for = {}
    maxy = 0
    for summary_name, datalist in data.items():
      x_vals = [x for x,y in datalist]
      y_vals_for[summary_name] = [y for x,y in datalist]
      maxy = max(maxy, max(y_vals_for[summary_name]))

    plt.axis([0, max(x_vals), -.01, 2 * maxy])
    plt.ylabel("Relative Error") 
    plt.xlabel("Summary size kb")   

    for (summary_name,y_vals),symb in zip(y_vals_for.items(), symbols):
      print summary_name,y_vals
      plt.plot(x_vals, y_vals, symb, label=summary_name)
      
    ax.legend()
    if OUT_TO_FILE:
      plt.savefig(distrib_name +"_accuracy.pdf")
      plt.close(fig)  
  
  return


if __name__ == '__main__':
  main()
