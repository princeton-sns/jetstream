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
  plot_data_over_time(data)

FLD_BW = 6
FLD_999 = -1
FLD_MY99 = -6
 
def parse_infile(infile):
  t_series = []
  cur_period_lat = []
  bw_series = []
  f = open(infile, 'r')
  t = 0
  for ln in f:
    if not 'IMGREPORT' in ln:
      continue
    fields = ln.strip().split(" ")
    my_lat_quant = int(fields[FLD_MY99].strip('.'))
    my_bw = int(fields[FLD_BW])
    t +=2
    t_series.append(t)
    cur_period_lat.append(my_lat_quant)
  f.close()
  return t_series,cur_period_lat


def plot_data_over_time(data):

  time,cur_period_lat = data
  
  figure, ax = plt.subplots()
  ax.plot(time, cur_period_lat)
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel('95th percentile latency (msec)', fontsize=22)

  
  if OUT_TO_FILE:
      plt.savefig("95th_percentile_lat.pdf")
      plt.close(figure)  


if __name__ == '__main__':
  main()
