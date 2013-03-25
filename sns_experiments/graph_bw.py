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
SAMPLE = True

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
                  help="output file name", default="bw_over_time_e1")
  parser.add_option("-l", "--latency", dest="latency",
                  help="latency file name")
  parser.add_option("-b", "--baseline", dest="baseline",
                  help="baseline file name")
  
  (options, args) = parser.parse_args()


  leg_artists = []
  figure, ax = plt.subplots()
  PLOT_LAT = options.latency is not None
  deg_label = "Degradation ratio" if PLOT_LAT else "Avg window size (secs)"
  EXP_MINUTES = 40 if PLOT_LAT else 7
  
  
  for infile in args:
 
    time_to_bw, time_to_tuples, level_transitions = parse_log(infile, PLOT_LAT)
  #  plot_src_tuples(time_to_tuples, ax, leg_artists) 
    offset = get_offset(time_to_bw)
    bw_seq = [ (tm,bytes) for tm, (bytes,tuples) in sorted(time_to_bw.items()) ]
    bw_seq = smooth_seq(bw_seq, offset, EXP_MINUTES * 60, window = 20)
    print "bw_seq", bw_seq[0:10]
    print "bw range is", bw_seq[0][0], " - ", bw_seq[-1][0]
  #  print "smoothed to",time_to_bw
    plot_bw(bw_seq, ax, leg_artists, "b.-") 

    if ALIGN:
      MAX_T = bw_seq[-1][0]
    else:
      MAX_T = max(bw_seq[-1][0])
      if len(time_to_tuples) > 0:
        MAX_T = max( MAX_T, max([t for (t,l) in time_to_tuples]))

    level_transitions = to_line(level_transitions, offset, MAX_T)
    plot_degradation(level_transitions, ax, leg_artists, deg_label, "k-")

  if options.baseline is not None:
    time_to_bw, _, _ = parse_log(options.baseline, PLOT_LAT)
    offset = get_offset(time_to_bw)
    bw_seq = [ (tm,bytes) for tm, (bytes,tuples) in sorted(time_to_bw.items()) ]
    bw_seq = smooth_seq(bw_seq, offset, EXP_MINUTES * 60)
    plot_bw(bw_seq, ax, leg_artists, "b--")   


  finish_plots(figure, ax, leg_artists, options.outfile,  \
      ["Bandwidth", "Degradation", "Bandwidth (no degradation)"])
  
  if options.latency:
    do_latency_bw_plot(bw_seq, offset, options, 0, MAX_T)
    
    

USE_BW_REP = False
BASE_H = 1000
def parse_log(infile, PLOT_LAT):
  time_to_bw = {}
  level_transitions = []
  time_to_tuples = []
  f = open(infile, 'r')
  hist_sizes = []
  for ln in f:
    if 'RootReport' in ln:
      try:
        fields = ln.split(" ")
        ts = long(fields[-9])
        window = long(float(fields[-6]))
        bw_sec = float(fields[-4])
        tuples_sec = float(fields[-2])
        if not PLOT_LAT:
          level_transitions.append (  (ts, window) )
        time_to_bw[ts] = (bw_sec, tuples_sec)
      except Exception as e:
        print str(e),ln
        sys.exit(0)
    if 'avg hist size' in ln:
        if not SAMPLE:
          h_size = int(ln.split(" ")[-5])
          level_transitions.append (  (ts, h_size) )
        else:
          h_size = float(ln.split(" ")[-1])
          level_transitions.append (  (ts, h_size) )


        
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

  if PLOT_LAT:  #really means "we are doing hist experiment
    BASE_H = level_transitions[0][1]
#    level_transitions = [(ts, (BASE_H - l) * 1000  ) for (ts,l) in level_transitions]
    if not SAMPLE:
      level_transitions = [(ts,  (float(BASE_H)  / l) ) for (ts,l) in level_transitions]
    #else
      #level_transitions = [(ts,  (float(BASE_H)  / l) ) for (ts,l) in level_transitions]
  else:
    level_transitions = [(ts, l / 1000 ) for (ts,l) in level_transitions]
  

  f.close()
  if len(hist_sizes) > 0:
    return time_to_bw,time_to_tuples, hist_sizes
  else:
    return time_to_bw,time_to_tuples,level_transitions


def get_offset(time_to_bw):
  for tm, (bytes,tuples) in sorted(time_to_bw.items()):
      if bytes > 0:
        return tm - 10  
  return 0        
        
def smooth_seq(my_seq, offset, max_len, window=10):
  val_list = []
  res = []
  for tm, val in my_seq:
    val_list.append(val)
    smoothed = sum( val_list[-window:])
    b = len(val_list[-window:])
    v = (tm-offset, smoothed / b) 
    res.append(  v)
    if tm - offset > max_len:
      break
  return res

def plot_bw(bw_seq, ax, leg_artists, line_fmt):
  time_data = []
  bw = []
  for tm, bytes, in bw_seq:
#    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append( datetime.datetime.fromtimestamp(tm))
    bw.append( 8 * bytes/ 1000)

  MAX_Y = max(bw)
  
  ax.set_ylim( 0, 1.3 *  MAX_Y)  
  
  plt.tick_params(axis='both', which='major', labelsize=16)
  bw_line, = ax.plot_date(time_data, bw, line_fmt, label="BW") 
  ax.set_xlabel('Time', fontsize=22)  
  ax.set_ylabel('Bandwidth (kbits/sec)', fontsize=22)
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
  last_level = level_transitions[0][1]
  revised = [(0, last_level)]
  for  (t, lev) in level_transitions:
    if t - min_time > max_time:
      break
    lev = float(lev)
    revised.append ( (t-1 - min_time, last_level))
    revised.append ( (t - min_time, lev ))
    last_level = lev 
#  print "would be appending",(max_time, last_level), " to " ,revised[-1]
  revised.append ( (max_time, last_level))
  return revised
  
def plot_degradation(level_transitions, old_ax, leg_artists, deg_label, line_fmt="k-"):

  ax = old_ax.twinx()
  time_data = [datetime.datetime.fromtimestamp(t) for t,l in level_transitions]
  lev_data = [l for t,l in level_transitions]
  deg_line, = ax.plot_date(time_data, lev_data, line_fmt) 
#  ax.set_ylim( 0, 1.2 *  max(lev_data))    
  ax.set_ylim( 0, max(lev_data) * 2)  
 
  ax.set_ylabel(deg_label, fontsize=22)
  leg_artists.append( deg_line )
#  print level_transitions  



    

def do_latency_bw_plot(time_to_bw, offset, options, min_time, max_time): 
    print "offset is ",offset   
    lat_series = get_latencies_over_time(options.latency, offset, min_time, max_time)
    lat_series = smooth_seq(lat_series, 0, 60 * 60, window=10)
    print lat_series[0:10]
    
    figure, ax = plt.subplots()
    leg_artists = []

    plot_bw(time_to_bw, ax, leg_artists, "b.-") 
    plot_latencies(lat_series, ax, leg_artists)
    finish_plots(figure, ax, leg_artists, options.outfile + "_latencies", ["Bandwidth", "Latency"])



def quantile(values, total, q):
  running_tally = 0
  for k,v in sorted(values.items()):
    running_tally += v
    if running_tally > total * q:
      return k
  return INFINITY



def get_latencies_over_time(infile, offset, min_time, max_time, QUANTILE = 0.5):
  f = open(infile, 'r')
  ret = defaultdict( dict ) # label --> bucket --> count
  for ln in f:
#    print ln
    _,ln = ln.split(':')  #ditch operator ID from echo output
    hostname, label, bucket, count = ln[2:-2].split(",")
    _, bucket = bucket.split("=");
    _, count = count.split("=")
#    label = " ".join(label.split(" ")[0:3])
#    if 'after' in label:
#      continue
    bucket = int(bucket)
    count = int(count)
    
    if 'after' in label:
      continue
      
    l = label.split(" ")[3].split(",")[0]
    label = int (l)
    
    if bucket in ret[label]:    
      ret[label][bucket] += count
    else:
      ret[label][bucket] = count
  f.close()
  
  ts_to_quant = []
  for l, buckets in sorted(ret.items()):
    t = l/1000 - offset
    if t < min_time or t > max_time:
      continue
    q = quantile(buckets, sum(buckets.values()), QUANTILE) 
    ts_to_quant.append (   (t, q))
  
  return ts_to_quant
  
def plot_latencies(lat_series, old_ax, leg_artists, line_fmt="k-"):
  print "plotting latency data"
  ax = old_ax.twinx()
  time_data = [datetime.datetime.fromtimestamp(t) for t,l in lat_series]
  latency_data = [l for t,l in lat_series]
  line, = ax.plot_date(time_data, latency_data, line_fmt) 
#  ax.set_ylim( 0, 1.2 *  max(lev_data))    
  ax.set_ylim( 0, max(latency_data) *1.2)  
 
  ax.set_ylabel('Avg latency (msecs)', fontsize=22)
  leg_artists.append( line )
  
  


def finish_plots(figure, ax, leg_artists, outname, label_strings):
  figure.subplots_adjust(left=0.15)
  figure.subplots_adjust(bottom=0.18)  
  figure.subplots_adjust(right=0.9)  
  labels = ax.get_xticklabels() 
  for label in labels: 
      label.set_rotation(30) 
      #"Src Records",   
  plt.legend(leg_artists, label_strings);
  plt.tick_params(axis='both', which='major', labelsize=16)
  
  if OUT_TO_FILE:
      plt.savefig(outname + ".pdf")
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
