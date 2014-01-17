from collections import defaultdict
import re
import sys
#import numpy
#import numpy.linalg
#from numpy import array
from math import log



OUT_TO_FILE = True
OUT_FILE = "key_stats" 
import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
#import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True


def main():
  infile = sys.argv[1]
  data = parse_data(infile)
  
  d2 = {}
  for t,(url, dom) in data.items():
    d2[t] = (url, dom, 18.0/5 * t)
  data = d2
#  plot_inverse_compressions(data, "keystats_inv")
#  for i in range(3):
#    plot_compressions(data, OUT_FILE + "_%d.pdf" % i, i)
  plot_compressions(data, OUT_FILE + ".pdf", 3)

  print "Done. Output in",OUT_FILE
  
INTERVAL_NAMES = {5: "5s", 60:"minute", 300:"5 m", 3600:"hour", 86400:"day"}

PLOT_IDEAL = False
def plot_compressions(data, out_file, when_to_stop):

  colors = ["#5555FF", "#aa0000", "#D0D0D0"]
  t_labels, url_series, dom_series, ideal_series = [], [], [], []
  for t, (u,d, i) in sorted(data.items()):
    url_series.append(u)
    dom_series.append(d)
    ideal_series.append(i)
    t_labels.append( INTERVAL_NAMES[t])
  
  if PLOT_IDEAL:
    top_pt_log = int(log( max( ideal_series )) / log(2) ) +1
  else:
    top_pt_log = int(log( max( dom_series )) / log(2) ) +1  

  fig = plt.figure(figsize=(6.25,3.5))
  fig.subplots_adjust(bottom=0.13)
  fig.subplots_adjust(top=0.99)
  fig.subplots_adjust(left=0.14)
  fig.subplots_adjust(right=0.98)
  
  ax = fig.add_subplot(111)
  ax.set_yscale('log',basey=2)
#  print "log of top-pt is
  ticks = [2 ** x for x in range(top_pt_log)]
  plt.yticks(ticks, ticks)
  
  plt.ylim((1, 2 ** top_pt_log))
      
  offset = 0.5
  if PLOT_IDEAL:
    params = zip([url_series, dom_series, ideal_series], [ "URLs", "Domains","Ideal"], colors)
  else:
    params = zip([url_series, dom_series], [ "URLs", "Domains"],  ["#5555FF", "#aa0000"] )

  cwidth = len(params) * offset + 0.3
  for (series,series_name,c) in params[0:when_to_stop+1]:
    positions = [x * cwidth + offset for x in range(len(data))]
    plt.bar(positions, series, width = 0.5, color = c, label=series_name, bottom=1) #log=True)
    offset += 0.5
  label_positions = [x * cwidth + 1 for x in range(len(data))]
  plt.xticks(label_positions, t_labels, fontsize = 10, rotation = 0)
  plt.xlabel("Aggregation time period")
  plt.ylabel("Relative savings from aggregation")
  
  ax.legend(frameon=False, loc = "upper left")
  if OUT_TO_FILE:
    plt.savefig(out_file)
    plt.close(fig)  

#  print data


def plot_inverse_compressions(data, out_file):

  colors = ["#5555FF", "#aa0000"]
  t_labels, url_series, dom_series = [], [], []
  for t, (u,d, i) in sorted(data.items()):
    url_series.append(u)
    dom_series.append(d)
    t_labels.append( INTERVAL_NAMES[t])


  print "not yet implemented"
  sys.exit(0)

  plt.xlabel("Aggregation time period")
  plt.ylabel("Data size")
  ax.legend(frameon=False, loc = "upper left")
  if OUT_TO_FILE:
    plt.savefig(out_file)
    plt.close(fig)  

def parse_data(infile):
  f = open(infile, 'r')
  data = {}
  for ln in f:
    t, url,dom = ln.strip().split()
    t,url, dom = int(t), float(url), float(dom)
    data[t] = (url,dom)
  f.close()
  return data
  
  
  
  

if __name__ == '__main__':
  main()
