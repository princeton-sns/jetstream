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
  
  suffix = ""
  if len(sys.argv) > 2:
    suffix = sys.argv[2]
  
  data = parse_infile(infile)
  series_timelen = (data[TIME][-1] - data[TIME][0])
  print "Done, %d level changes over %d secs" % (len(data[TIME]),series_timelen)
  avg_time_at_level = series_timelen / float(len(data[TIME]))
  print "Average time-at-level %0.2f secs" % (avg_time_at_level)
  data[TIME], data[LEVEL] = flatten_lines(data[TIME], data[LEVEL])
  plot_series(data, LEVEL, "local_deg_" + suffix + ".pdf")


TIME = "Time"
LEVEL = "Level"
FIELDS_TO_PLOT = {
  TIME: -1,
  LEVEL: 10
}  

def parse_infile(infile):
  data = {}
  for field,offset in FIELDS_TO_PLOT.items():
    data[field] = []


  f = open(infile, 'r')
  for ln in f:
    if not 'setting degradation level' in ln:
      continue
      
    fields = ln.strip().replace("/"," ").split(" ")
    for field,offset in FIELDS_TO_PLOT.items():
      val = float(fields[offset])
      data[field].append(val)
  f.close()  
  return data


def flatten_lines(time, series):
  newtime = []
  newseries = []
  
  prev_val = series[0]
  newtime.append(time[0])
  newseries.append(series[0])
  for t,v in zip(time, series)[1:]:
    newseries.append(prev_val)
    newtime.append(t-1)
    prev_val = v
    newseries.append(v)
    newtime.append(t)

  return newtime[0:90], newseries[0:90]

def get_x_from_time(t):
  return datetime.datetime.fromtimestamp(t )
  
def plot_series(data, seriesname, filename):  

  time = [get_x_from_time(x ) for x in data['Time']]
  series_to_plot = data[seriesname]

#  legend_artists = []

  figure, ax = plt.subplots()

  figure.autofmt_xdate()
  line, = ax.plot_date(time, series_to_plot, 'b-')
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

  ax.set_title("Degradation level (lower = more degraded)")
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel(seriesname, fontsize=22)
  ax.set_ylim( 0, 1.2 * max(series_to_plot))  
  
  if OUT_TO_FILE:
      plt.savefig(filename)
      plt.close(figure)  


if __name__ == '__main__':
  main()
  