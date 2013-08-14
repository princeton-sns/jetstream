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


MEDIAN_LAT = "Median latency"
MY_LAT = "95th percentile latency"
FIELDS_TO_PLOT_OLD = {
  "BW": 6,
  MEDIAN_LAT: 14,
  MY_LAT: -6,
  "99.9th percentile latency": -1
}

FIELDS_TO_PLOT = {
  "Time": 0,
  "BW": 1,
  MEDIAN_LAT: 5,
  MY_LAT: 7,
  "99.9th percentile latency": 9,
  "BW-deviation": 11
}
 
def parse_infile(infile):
  data = {}
  for field,offset in FIELDS_TO_PLOT.items():
    data[field] = []
  f = open(infile, 'r')
  t = 0
  t_series = []
  for ln in f:
#    if not 'IMGREPORT' in ln:
#      continue
      
    fields = ln.strip().split(" ")
    for field,offset in FIELDS_TO_PLOT.items():
      val = float(fields[offset].strip('.'))
      data[field].append( val)
  f.close()
  return data


def plot_data_over_time(data):

  time = [x / 1000 for x in data['Time']]
  med_series = data[MEDIAN_LAT]
  myquant = data[MY_LAT]
  bw_series = [x / 1000000 for x in data['BW']]
  
  figure, ax = plt.subplots()
  ax.plot(time, myquant)
  ax2 = ax.twinx()
  ax2.plot(time, bw_series, "r.")  
  
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel('99.9th percentile latency (msec)', fontsize=22)
  ax2.set_ylabel('Bandwidth (mbytes/sec)', fontsize=22)

  
  if OUT_TO_FILE:
      plt.savefig("img_latency.pdf")
      plt.close(figure)  


if __name__ == '__main__':
  main()
