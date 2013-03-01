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
  time_to_bw, level_transitions = parse_log(infile)
  
  leg_artists = []
  figure = plot_bw(time_to_bw, leg_artists)  
  ax = figure.add_subplot(111)

  level_transitions = to_line(level_transitions, min(time_to_bw.keys()), max(time_to_bw.keys()))
  
  plot_degradation(level_transitions, ax, leg_artists)


  finish_plots(figure, ax, leg_artists)

def finish_plots(figure, ax, leg_artists):
  plt.xlabel('Time', fontsize=24)
  labels = ax.get_xticklabels() 
  for label in labels: 
      label.set_rotation(30)   
  plt.legend(leg_artists, ["BW", "Degradation"]);
  
  if OUT_TO_FILE:
      plt.savefig("bw_over_time_e1.pdf")
      plt.close(figure)  


def parse_log(infile):
  time_to_bw = {}
  level_transitions = []
  f = open(infile, 'r')
  for ln in f:
    if 'BWReporter' in ln:
      fields = ln.split(" ")
      bytes = float(fields[ -4 ])
      tuples = float(fields[ -2 ])
      tstamp = long(fields[-5])
      time_to_bw[tstamp] = (bytes, tuples)
    elif 'setting degradation level' in ln:
      fields = ln.split(" ")
      level, ts = int(fields[-5].rstrip(',')), long(fields[-1])
      level_transitions.append (  (ts, level) )

  f.close()
  return time_to_bw,level_transitions

def plot_bw(time_to_bw, leg_artists):
  time_data = []
  bw = []
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
#    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append( datetime.datetime.fromtimestamp(tm))
    bw.append(bytes)

  MAX_Y = max(bw)

  fig, ax = plt.subplots()
  
  ax.set_ylim( 0, 1.2 *  MAX_Y)  
  bw_line, = ax.plot_date(time_data, bw, "b.-", label="BW") 
  ax.set_ylabel('BW (bytes)', fontsize=24)
  leg_artists.append( bw_line )
  return fig
  

def to_line(level_transitions, min_time, max_time):
  last_level = 3
  revised = [(min_time, last_level)]
  for  (t, lev) in level_transitions:
    revised.append ( (t-1, last_level))
    revised.append ( (t, lev))
    last_level = lev
  revised.append ( (max_time, last_level))
  return revised
  
def plot_degradation(level_transitions, old_ax, leg_artists):

  ax = old_ax.twinx()
  time_data = [datetime.datetime.fromtimestamp(t) for t,l in level_transitions]
  lev_data = [l for t,l in level_transitions]
  deg_line, = ax.plot_date(time_data, lev_data, "k-", label="Degradation") 
  ax.set_ylim( 0, 1.2 *  max(lev_data))  
  ax.set_ylabel('Degradation level', fontsize=24)
  leg_artists.append( deg_line )
#  print level_transitions  
  
if __name__ == '__main__':
  main()
