from collections import defaultdict
import datetime
import re
import sys
import time

import numpy
import numpy.linalg
from numpy import array
from optparse import OptionParser



OUT_TO_FILE = True
ALIGN = True

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True



def main():

  parser = OptionParser()

  parser.add_option("-o", "--output", dest="outfile",
                  help="output file name", default="bw_over_time_e1.pdf")
  (options, args) = parser.parse_args()



  leg_artists = []
  figure, ax = plt.subplots()

  for infile in args:
 
    time_to_bw, time_to_tuples, level_transitions = parse_log(infile)
  #  plot_src_tuples(time_to_tuples, ax, leg_artists) 
    time_to_bw,offset = smooth_bw(time_to_bw)
    print "bw range is", min(time_to_bw.keys()), " - ", max(time_to_bw.keys())
  #  print "smoothed to",time_to_bw
    plot_bw(time_to_bw, ax, leg_artists) 

    if ALIGN:
      MAX_T = 7 * 60
    else:
      MAX_T = max(time_to_bw.keys())
      if len(time_to_tuples) > 0:
        MAX_T = max( MAX_T, max([t for (t,l) in time_to_tuples]))
    level_transitions = to_line(level_transitions, offset, MAX_T)
    plot_degradation(level_transitions, ax, leg_artists)

  finish_plots(figure, ax, leg_artists, options.outfile)

USE_BW_REP = False
def parse_log(infile):
  time_to_bw = {}
  level_transitions = []
  time_to_tuples = []
  f = open(infile, 'r')
  for ln in f:
    if 'RootReport' in ln:
      try:
        fields = ln.split(" ")
        ts = long(fields[-9])
        window = long(float(fields[-6]))
        bw_sec = float(fields[-4])
        level_transitions.append (  (ts, window) )
        time_to_bw[ts] = (bw_sec, 0)
      except Exception as e:
        print str(e),ln
        sys.exit(0)
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


def smooth_bw(time_to_bw):
  last_bytes = []
  res = {}
  offset = 0
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
    if offset == 0:
      if bytes > 0:
        offset = tm - 10
      else:
        continue
    last_bytes.append(bytes)
    smoothed = sum( last_bytes[-10:])
    b = len(last_bytes[-10:])
    res[tm-offset] = ((smoothed) / b, 0)
    if tm - offset > 7 * 60:
      break
  return res,offset

def plot_bw(time_to_bw, ax, leg_artists):
  time_data = []
  bw = []
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
#    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append( datetime.datetime.fromtimestamp(tm))
    bw.append( 8 * bytes)

  MAX_Y = max(bw)
  
  ax.set_ylim( 0, 1.2 *  MAX_Y)  
  
  plt.tick_params(axis='both', which='major', labelsize=16)
  bw_line, = ax.plot_date(time_data, bw, "b.-", label="BW") 
  ax.set_xlabel('Time', fontsize=22)  
  ax.set_ylabel('Bandwidth (bits/sec)', fontsize=22)
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
  last_level = 1
  revised = [(0, last_level)]
  for  (t, lev) in level_transitions:
    if t - min_time > max_time:
      break
    lev = float(lev) / 1000
    revised.append ( (t-1 - min_time, last_level))
    revised.append ( (t - min_time, lev ))
    last_level = lev 
  print "would be appending",(max_time, last_level), " to " ,revised[-1]
  revised.append ( (max_time, last_level))
  return revised
  
def plot_degradation(level_transitions, old_ax, leg_artists):

  ax = old_ax.twinx()
  time_data = [datetime.datetime.fromtimestamp(t) for t,l in level_transitions]
  lev_data = [l for t,l in level_transitions]
  deg_line, = ax.plot_date(time_data, lev_data, "k-") 
#  ax.set_ylim( 0, 1.2 *  max(lev_data))    
  ax.set_ylim( 0, 30)  
 
  ax.set_ylabel('Avg window size (secs)', fontsize=22)
  leg_artists.append( deg_line )
#  print level_transitions  



def finish_plots(figure, ax, leg_artists, outname):
  figure.subplots_adjust(left=0.15)
  figure.subplots_adjust(bottom=0.18)  
  labels = ax.get_xticklabels() 
  for label in labels: 
      label.set_rotation(30) 
      #"Src Records",   
  plt.legend(leg_artists, ["Bandwidth", "Degradation"]);
  plt.tick_params(axis='both', which='major', labelsize=16)
  
  if OUT_TO_FILE:
      plt.savefig(outname)
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
