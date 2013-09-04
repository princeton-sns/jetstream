from collections import defaultdict
import datetime
import re
import sys
import time

import numpy
import numpy.linalg
from numpy import array

OUT_TO_FILE = True


DATE = True


import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True




MEDIAN_LAT = "Median"
MY_LAT = "95th percentile in period"
LAT_999 = "99.9th percentile latency (msec)"
GLOBAL_DEVIATION = "BW-deviation"
IMAGE_COUNT = "Images per period"
COEF_VAR = "Coefficient of variation"
TDELTA = "Time deltas"
LAT_MAX = "Max latency in period"

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
  GLOBAL_DEVIATION: 11,
  LAT_MAX: 13
}
 

def main():
  infile = sys.argv[1]
  
  data,counts_by_name = parse_infile(infile)
  data[TDELTA] = get_time_deltas(data['Time'])  
  data["BW"]  = [x/d for x,d in zip(smooth_seq(data["BW"], window=4), data[TDELTA])]
  data[IMAGE_COUNT]  = smooth_seq(data[IMAGE_COUNT], window=5)
  data[COEF_VAR] = stddev_to_c_of_v(data)

  print "   Worker               Total Bytes Sent\n" + "-"* 40
  for c,name in counts_by_name:
    print " %s    %d" % (name,c)

#  plot_data_over_time(data, MY_LAT, "latency_local.pdf")
#  plot_data_over_time(data, MEDIAN_LAT, "latency_median.pdf")
  plot_data_over_time(data, [COEF_VAR], "internode_variation.pdf")
  plot_data_over_time(data, [LAT_999], "latency_highquant.pdf")
  plot_data_over_time(data, [LAT_MAX], "latency_extremum.pdf")
  plot_data_over_time(data, [MEDIAN_LAT, MY_LAT], "latency_multi.pdf")


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
  
def get_time_deltas(timeseries):
  deltas = []
  prev = timeseries[0]
  for t in timeseries[1:]:
    deltas.append(t - prev)
    prev = t
  deltas.append( timeseries[-1] ) #pad at end
  return deltas
  
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
  verylates = 0
  t_series = []
  names = {}
  last_by_node = None
  for ln in f:
    if 'BYNODE' in ln:
      last_by_node = ln
      continue
    if 'VERYLATE' in ln:
      verylates +=1
      continue
    if 'INDEX' in ln:
       fields = ln.strip().split(" ")
       names[int(fields[1])] = fields[-1]
       continue
      
      
    fields = ln.strip().split(" ")
    for field,offset in FIELDS_TO_PLOT.items():
      val = float(fields[offset].strip('.'))
      data[field].append( val)
  f.close()
  
  if last_by_node:
    counts_by_node = [int(x) for x in last_by_node[8:].split()]
    counts_by_name = make_counts_by_name(names, counts_by_node)
    print "Asymmetry ratio %0.2f%%; %d periods with very late data"  %\
        (asymmetry(counts_by_node), verylates)
  else:
    counts_by_name = {}
  print "Data ends at %s." % (time.ctime(data['Time'][-1]/1000))
  
  return data, counts_by_name


def make_counts_by_name(names, counts_by_node):
  named_tuples = []
  for count, idx in zip(counts_by_node, range(0,len(counts_by_node))):
    named_tuples.append( (count, names[idx]))
  named_tuples.sort()
  return named_tuples

def asymmetry(data):
  """ Result is (max - min) / max or 1 - min/max. 50% means max is twice min."""
  data.sort()
  return (data[-1] - data[0]) / float(data[-1]) * 100.0

def get_x_from_time(t):
  if DATE:
    return datetime.datetime.fromtimestamp(t )
  else:
    return t

linelabels = ['b-', 'gx']

def plot_data_over_time(data, seriesnames, filename):

  time = [get_x_from_time(x / 1000) for x in data['Time']]
  bw_series = [x / 1000 for x in data['BW']] #already got a factor of a thousand because time is in millis
  legend_artists = []
#  series_to_plot = 
  
  figure, ax = plt.subplots()

  figure.autofmt_xdate()
  ax.set_ylim( 0, 1.2 * max([max(data[sname]) for sname in seriesnames]))  
  
  for seriesname,linelabel in zip(seriesnames, linelabels):
    line, = ax.plot_date(time, data[seriesname], linelabel)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    legend_artists.append( line )
  
  ax2 = ax.twinx()
  line, = ax2.plot_date(time, bw_series, "r.")  
  legend_artists.append( line )  
  
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  if len(seriesname) == 1:
    ax.set_ylabel("Latency (msec)", fontsize=22)
  else:
    ax.set_ylabel("Latency (msec)", fontsize=22)

  ax2.set_ylabel('Bandwidth (mbytes/sec)', fontsize=22)
  ax2.set_ylim( 0, 1.2 * max(bw_series))  

  plt.legend(legend_artists, [seriesname, "Bandwidth"], loc="center", bbox_to_anchor=(0.5, 1.05), frameon=False, ncol=2);
  
  plt.savefig(filename)
  plt.close(figure)  


if __name__ == '__main__':
  main()
