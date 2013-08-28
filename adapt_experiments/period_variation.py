
from collections import defaultdict
import datetime
import re
import sys
import time


OUT_TO_FILE = True
X_AXIS_WITH_DATES = True

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True


def main():
  infile = sys.argv[1]
  
  data = parse_infile(infile)
  series_timelen = (data[TIME][-1] - data[TIME][0])/1000
  print "Done, %d data points over %d secs" % (len(data[TIME]),series_timelen)
  
  plot_series(data, PACKETS_IN, 'packets_in.pdf')
  plot_series(data, SIGNAL, 'signal.pdf')
  plot_predict_accuracy(data[TIME], data[SIGNAL], 'accuracy_cdf.pdf')

TIME = "Time"
SIGNAL = "Signal"
PACKETS_IN = "Packets received"
FIELDS_TO_PLOT = {
  TIME: 0,
  SIGNAL: 2,
  PACKETS_IN: -3
}

def parse_infile(infile):
  data = {}
  for field,offset in FIELDS_TO_PLOT.items():
    data[field] = []
  f = open(infile, 'r')
  for ln in f:      
      
    fields = ln.strip().split(" ")
    for field,offset in FIELDS_TO_PLOT.items():
      val = float(fields[offset].strip('.'))
      data[field].append( val)
  f.close()
  return data


def get_x_from_time(t):
  if X_AXIS_WITH_DATES:
    return datetime.datetime.fromtimestamp(t )
  else:
    return t


def plot_series(data, seriesname, filename):  

  time = [get_x_from_time(x / 1000) for x in data['Time']]
  series_to_plot = data[seriesname]

#  legend_artists = []

  figure, ax = plt.subplots()

  figure.autofmt_xdate()
  line, = ax.plot_date(time, series_to_plot, 'b-')
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel(seriesname, fontsize=22)
  ax.set_ylim( 0, 1.2 * max(series_to_plot))  
  if OUT_TO_FILE:
      plt.savefig(filename)
      plt.close(figure)  


TRAIL_WIN = 5
HEAD_WIN = 10
def  plot_predict_accuracy(timeseries, dataseries, filename):

  ratios = []
  for i in range(TRAIL_WIN, len(timeseries) - HEAD_WIN):
    past_avg = sum( dataseries[i-TRAIL_WIN:i] ) / TRAIL_WIN
    future_avg = sum( dataseries[i:i+HEAD_WIN] )/ HEAD_WIN
    if past_avg == 0:
      if future_avg > 0:
        ratios.append(10)
      else:
        ratios.append(1)
    else:
      ratios.append( float(future_avg) / past_avg )
    
  figure, ax = plt.subplots()
  ratios.sort()
  xseries = [ x * 100.0/ len(ratios) for x in range(0, len(ratios))]  
  ax.plot( xseries, ratios)
  ax.set_xlim(0, 100)
  ax.set_ylim(0, 4)

  ax.set_ylabel("Future/past bw ratio")
  ax.set_xlabel("Percentile")

  plt.savefig(filename)
  plt.close(figure)  



if __name__ == '__main__':
  main()