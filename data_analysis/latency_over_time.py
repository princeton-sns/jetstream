from collections import defaultdict
import datetime
import re
import sys
import time

from optparse import OptionParser

import numpy
import numpy.linalg
from numpy import array

OUT_TO_FILE = True
DATE = False

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True


MEDIAN_LAT = "Median"
MY_LAT = "95th percentile"
LAT_999 = "99.9th percentile latency (msec)"
GLOBAL_DEVIATION = "BW-deviation"
IMAGE_COUNT = "Images per period"
COEF_VAR = "Coefficient of variation"
TDELTA = "Time deltas"
LAT_MAX = "Maximum in period"

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
  parser = OptionParser()
  parser.add_option("-o", "--output", dest="out_dir",
                  help="output directory name", default="")
  parser.add_option("-e", "--etime", dest="experiment_time", action="store_true",
                  help="experiment starts at t = 0", default=False)
                  
  (options, args) = parser.parse_args()

  out_dir = options.out_dir
  if len(out_dir) > 0 and out_dir[-1] != '/':
     out_dir =  out_dir + "/" 

  infile = args[0]
  
  data,counts_by_name = parse_infile(infile)
  
  if options.experiment_time:
    t_offset = data['Time'][0] # - 5 * 60 * 60 * 1000 #change time zone
    data['Time'] = [x -t_offset for x in data['Time']]
  
  for lat in [LAT_999, MEDIAN_LAT, MY_LAT, LAT_MAX]:
    data[lat] = [x/1000.0 for x in data[lat]]
  
  data[TDELTA] = get_time_deltas(data['Time'])  
  data["BW"]  = [x/d for x,d in zip(smooth_seq(data["BW"], window=5), data[TDELTA])]
  data[MEDIAN_LAT]  = smooth_seq(data[MEDIAN_LAT], window=5)
  data[COEF_VAR] = stddev_to_c_of_v(data)

  print "   Worker               Total Bytes Sent\n" + "-"* 40
  for c,name in counts_by_name:
    print " %s    %d" % (name,c)

#  plot_data_over_time(data, MY_LAT, "latency_local.pdf")
#  plot_data_over_time(data, MEDIAN_LAT, "latency_median.pdf")
  plot_data_over_time(data, [COEF_VAR], out_dir+ "internode_variation.pdf")
  plot_data_over_time(data, [LAT_999], out_dir + "latency_highquant.pdf")
  plot_data_over_time(data, [LAT_MAX], out_dir + "latency_extremum.pdf")
  plot_data_over_time(data, [MEDIAN_LAT, MY_LAT, LAT_MAX], out_dir + "latency_multi.pdf")
  plot_bw(data, out_dir + "img_bw_over_time.pdf")

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

linelabels = ['k-', 'b.', 'gx']

def plot_data_over_time(data, seriesnames, filename):

  legend_artists = []
#  series_to_plot = 
  
  figure, ax = plt.subplots()

  
  if DATE:
    figure.autofmt_xdate()
    time = [get_x_from_time(x / 1000) for x in data['Time']]
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
  else:
    time = [ x / (60 * 1000) for x in data['Time']]
    
  MAX_Y = 1.15 * max([max(data[sname]) for sname in seriesnames])
  ax.set_ylim( 0, MAX_Y)  
  
  for seriesname,linelabel in zip(seriesnames, linelabels):
    line, = ax.plot(time, data[seriesname], linelabel)
    legend_artists.append( line )
  
  ax.set_xlabel('Elapsed time (minutes)', fontsize=18)  
  if len(seriesnames) == 1:
    ax.set_ylabel(seriesnames[0], fontsize=18)
  else:
    ax.set_ylabel("Latency (sec)", fontsize=18)

  plt.yticks(numpy.arange(0, MAX_Y, 2))
  ax.tick_params(axis='both', which='major', labelsize=15)


  leg_labels = []
  leg_labels.extend(seriesnames)
  leg_labels.reverse()
  legend_artists.reverse()
  plt.legend(legend_artists, leg_labels, loc="center", bbox_to_anchor=(0.5, 0.9), frameon=False, ncol=2);

  figure.set_size_inches(6.25, 4.25)
  figure.subplots_adjust(left=0.10)
  figure.subplots_adjust(right=0.97)  
  figure.subplots_adjust(bottom=0.16)
  figure.subplots_adjust(top=0.99)
  
  plt.savefig(filename)
  plt.close(figure)  


def plot_bw(data, filename):
  bw_series = [ 8 * x / 1000 for x in data['BW']] #already got a factor of a thousand because time is in millis
  figure, ax = plt.subplots()

  if DATE:
    time = [get_x_from_time(x / 1000) for x in data['Time']]
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    figure.autofmt_xdate()

  else:
    time = [ x / (60 * 1000) for x in data['Time']]


  line, = ax.plot(time, bw_series, "r.")  
  ax.set_xlabel('Experiment time (minutes)', fontsize=18)  
  ax.set_ylabel('Bandwidth (Mbits/sec)', fontsize=18)
  ax.set_ylim( 0, 1.2 * max(bw_series))  
  ax.tick_params(axis='both', which='major', labelsize=15)

  figure.set_size_inches(6.25, 3.5) 
  figure.subplots_adjust(bottom=0.17)  
  figure.subplots_adjust(top=0.98)
  figure.subplots_adjust(left=0.12)
  figure.subplots_adjust(right=0.97)
  plt.savefig(filename)
  plt.close(figure)  


if __name__ == '__main__':
  main()
