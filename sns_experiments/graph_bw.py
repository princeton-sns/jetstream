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
  time_to_bw, time_to_tuples, level_transitions = parse_log(infile)
  
  leg_artists = []
  figure, ax = plt.subplots()
  
#  plot_src_tuples(time_to_tuples, ax, leg_artists) 
  plot_bw(time_to_bw, ax, leg_artists) 

  MAX_T = max(time_to_bw.keys())
  if len(time_to_tuples) > 0:
    MAX_T = max( MAX_T, max([t for (t,l) in time_to_tuples]))
  level_transitions = to_line(level_transitions, min(time_to_bw.keys()), MAX_T)
  plot_degradation(level_transitions, ax, leg_artists)

  finish_plots(figure, ax, leg_artists)

USE_BW_REP = False
def parse_log(infile):
  time_to_bw = {}
  level_transitions = []
  time_to_tuples = []
  f = open(infile, 'r')
  for ln in f:
    if 'RootReport' in ln:
      fields = ln.split(" ")
      ts = long(fields[-7])
      window = long(fields[-4])
      bw_sec = float(fields[-2])
      level_transitions.append (  (ts, window) )
      time_to_bw[ts] = (bw_sec, 0)
#      print zip(fields, range(0, 15))
#      sys.exit(0)
#     elif 'setting degradation level' in ln:
#       fields = ln.split(" ")
#       level, ts = int(fields[10].rstrip(',')), long(fields[-1])
#       level_transitions.append (  (ts, level) )
#     elif 'Tally in window' in ln:
#       fields = ln.split(" ")
#       ts, count = long(fields[-6]), int(fields[-3])     
#       time_to_tuples.append (  (ts, count) )
       

  f.close()
  return time_to_bw,time_to_tuples,level_transitions

def plot_bw(time_to_bw, ax, leg_artists):
  time_data = []
  bw = []
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
#    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append( datetime.datetime.fromtimestamp(tm))
    bw.append(bytes)

  MAX_Y = max(bw)
  
  ax.set_ylim( 0, 1.2 *  MAX_Y)  
  bw_line, = ax.plot_date(time_data, bw, "b.-", label="BW") 
  ax.set_ylabel('BW (bytes)', fontsize=24)
  leg_artists.append( bw_line )


def  plot_src_tuples(time_to_count, ax, leg_artists):
#  print time_to_count
  
  counts = [y for (_,y) in time_to_count]
  times = [datetime.datetime.fromtimestamp(t) for (t,_) in time_to_count]
  MAX_Y = max(counts)
  
  ax.set_ylim( 0, 1.2 *  MAX_Y)  
  line, = ax.plot_date(times,counts, "g.-") 
  ax.set_ylabel('Source records/sec', fontsize=24)
  leg_artists.append( line )  
  return
  

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
  deg_line, = ax.plot_date(time_data, lev_data, "k-") 
  ax.set_ylim( 0, 1.2 *  max(lev_data))  
  ax.set_ylabel('Degradation level', fontsize=24)
  leg_artists.append( deg_line )
#  print level_transitions  



def finish_plots(figure, ax, leg_artists):
  plt.xlabel('Time', fontsize=24)
  labels = ax.get_xticklabels() 
  for label in labels: 
      label.set_rotation(30) 
      #"Src Records",   
  plt.legend(leg_artists, ["BW", "Degradation"]);
  
  if OUT_TO_FILE:
      plt.savefig("bw_over_time_e1.pdf")
      plt.close(figure)  

# 
#     if USE_BW_REP and 'BWReporter' in ln:
#       fields = ln.split(" ")
#       bytes = float(fields[ -4 ])
#       tuples = float(fields[ -2 ])
#       tstamp = long(fields[-5])
#       time_to_bw[tstamp] = (bytes, tuples)
#     elif not USE_BW_REP and 'window@' in ln:
#       fields = ln.split(" ")
#       tstamp = long(fields[7])
#       bytes = float( fields[-2])
#       time_to_bw[tstamp] = (bytes, 0)

  
if __name__ == '__main__':
  main()
