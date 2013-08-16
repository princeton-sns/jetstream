from collections import defaultdict
import datetime

import re
import sys
import numpy
import numpy.linalg
from numpy import array



OUT_TO_FILE = True


DATE = True


import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True




MEDIAN_LAT = "Median latency"
MY_LAT = "95th percentile latency (msec)"
LAT_999 = "99.9th percentile latency (msec)"
GLOBAL_DEVIATION = "BW-deviation"
IMAGE_COUNT = "Images per period"
COEF_VAR = "Coefficient of variation"
FIELDS_TO_PLOT_OLD = {
  "BW": 6,
  MEDIAN_LAT: 14,
  MY_LAT: -6,
  LAT_999: -1
}

FIELDS_TO_PLOT = {
  "Time": 0,
  "BW": 1,
  IMAGE_COUNT: 3,
  MEDIAN_LAT: 5,
  MY_LAT: 7,
  LAT_999: 9,
  GLOBAL_DEVIATION: 11
}
 

def main():
  infile = sys.argv[1]
  
  data = parse_infile(infile)
  data["BW"]  = smooth_seq(data["BW"], window=20)
  data[IMAGE_COUNT]  = smooth_seq(data[IMAGE_COUNT], window=5)
  data[COEF_VAR] = stddev_to_c_of_v(data)

  plot_data_over_time(data, MY_LAT, "latency_local.pdf")
  plot_data_over_time(data, MEDIAN_LAT, "latency_median.pdf")
  plot_data_over_time(data, COEF_VAR, "internode_variation.pdf")


def stddev_to_c_of_v(data):
  r = []
  cumsum_bw = 0
  for bw, dev in zip(data["BW"], data[GLOBAL_DEVIATION]):
    cumsum_bw += bw
    if cumsum_bw == 0:
      r.append(0)
    else:
      r.append(  dev / cumsum_bw) 
  return r

def smooth_seq(my_seq, window=10):
  val_list = []
  res = []
  for val in my_seq:
    val_list.append(val)
    smoothed = sum( val_list[-window:])
    b = len(val_list[-window:])
    v = smoothed / b
    res.append(  v)
#    if tm - offset > max_len:
#      break
  return res

def parse_infile(infile):
  data = {}
  for field,offset in FIELDS_TO_PLOT.items():
    data[field] = []
  f = open(infile, 'r')
  t = 0
  t_series = []
  for ln in f:
    if 'BYNODE' in ln or 'VERYLATE' in ln:
       continue
      
    fields = ln.strip().split(" ")
    for field,offset in FIELDS_TO_PLOT.items():
      val = float(fields[offset].strip('.'))
      data[field].append( val)
  f.close()
  return data


def get_x_from_time(t):
  if DATE:
    return datetime.datetime.fromtimestamp(t )
  else:
    return t


def plot_data_over_time(data, seriesname, filename):

  time = [get_x_from_time(x / 1000) for x in data['Time']]
  myquant = data[seriesname]
  bw_series = [x / 1000000 for x in data['BW']]
  legend_artists = []
  
  figure, ax = plt.subplots()
  line, = ax.plot_date(time, myquant, 'b-')
  legend_artists.append( line )
  
  ax2 = ax.twinx()
  line, = ax2.plot_date(time, bw_series, "r.")  
  legend_artists.append( line )  
  
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel(seriesname, fontsize=22)
  ax2.set_ylabel('Bandwidth (mbytes/sec)', fontsize=22)


  plt.legend(legend_artists, [seriesname, "Bandwidth"], loc="center", bbox_to_anchor=(0.5, 1.05), frameon=False, ncol=2);

  
  if OUT_TO_FILE:
      plt.savefig(filename)
      plt.close(figure)  


if __name__ == '__main__':
  main()
