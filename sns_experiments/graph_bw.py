from collections import defaultdict
import datetime
import re
import sys
import time

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
  time_to_bw = parse_log(infile)
  figure = plot_bw(time_to_bw)

  if OUT_TO_FILE:
      plt.savefig("bw_over_time_e1.pdf")
      plt.close(figure)  




def parse_log(infile):
  time_to_bw = {}
  f = open(infile, 'r')
  for ln in f:
    if 'BWReporter' in ln:
      fields = ln.split(" ")
      bytes = int(fields[ -4 ])
      tuples = int(fields[ -2 ])
      tstamp = long(fields[-5]) / 1000
      time_to_bw[tstamp] = (bytes, tuples)

  f.close()
  return time_to_bw

def plot_bw(time_to_bw):
  time_data = []
  bw = []
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append( datetime.datetime.fromtimestamp(tm))
    bw.append(bytes)

  MAX_Y = max(bw)

  fig, ax = plt.subplots()
  
  plt.ylim( 0, 1.2 *  MAX_Y)  
  ax.plot_date(time_data, bw, "b.-") 
  plt.ylabel('BW (bytes)', fontsize=24)
  plt.xlabel('Time', fontsize=24)



  return fig
  
if __name__ == '__main__':
  main()
