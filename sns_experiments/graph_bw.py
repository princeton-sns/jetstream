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
SAMPLE = False
DATE = False

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
#matplotlib.rcParams['font.size'] = 10


def main():
  global EXP_MINUTES
  parser = OptionParser()

  parser.add_option("-o", "--output", dest="outfile",
                  help="output file name", default="bw_over_time_e1")
  parser.add_option("-l", "--latency", dest="latency",
                  help="latency file name")
  parser.add_option("-n", "--latency_no_degradation", dest="latency_no_degradation",
                  help="latency with no degredation file name")
  parser.add_option("-b", "--baseline", dest="baseline",
                  help="baseline file name")
  parser.add_option("-s", "--srcbw", dest="plot_srcbw", action="store_true", 
        default=False, help="plot source bw")
  
  
  
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

    if ALIGN:
      MAX_T = bw_seq[-1][0]
    else:
      MAX_T = max(bw_seq[-1][0])
      if len(time_to_tuples) > 0:
        MAX_T = max( MAX_T, max([t for (t,l) in time_to_tuples]))

    lin_level_transitions = to_line(level_transitions, offset, MAX_T)
    plot_degradation(lin_level_transitions, ax, leg_artists, deg_label, "k-")

    plot_bw(bw_seq, ax, leg_artists, "b.-") 


  LEGEND_LABELS =  ["Degradation"]
  LEGEND_LABELS.append("Receive rate")
  if options.plot_srcbw:
    src_bw = get_src_bw(bw_seq, level_transitions, offset)
    plot_bw(src_bw, ax, leg_artists, "r--")
    plt.setp(leg_artists[-1], linewidth=2)
    LEGEND_LABELS.append("Generation rate")
    leg_artists = [leg_artists[0], leg_artists[2], leg_artists[1]]
    LEGEND_LABELS = [LEGEND_LABELS[0], LEGEND_LABELS[2], LEGEND_LABELS[1]]
    
  
  #leg_artists.reverse()
  #LEGEND_LABELS.reverse()    

  if options.baseline is not None:
    time_to_bw, _, _ = parse_log(options.baseline, PLOT_LAT)
    offset = get_offset(time_to_bw)
    bw_seq = [ (tm,bytes) for tm, (bytes,tuples) in sorted(time_to_bw.items()) ]
    bw_seq = smooth_seq(bw_seq, offset, EXP_MINUTES * 60)
    plot_bw(bw_seq, ax, leg_artists, "b--")   
    LEGEND_LABELS.append("Bandwidth (no degradation)")    
    leg_artists.reverse()
    LEGEND_LABELS.reverse()


  ax.tick_params(axis='x', which='major', pad=10) #controlls the x 
  finish_plots(figure, ax, leg_artists, options.outfile, LEGEND_LABELS, legend_loc=2)

  
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
          h_size = int(ln.split(" ")[-1])
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
    if not SAMPLE:
      level_transitions = [(ts,  l / float(BASE_H)) for (ts,l) in level_transitions]
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

def get_x_from_time(t):
  if DATE:
    return datetime.datetime.fromtimestamp(t + 60 * 60 * 5)
  else:
    return t


def plot_bw(bw_seq, ax, leg_artists, line_fmt):
  time_data = []
  bw = []
  for tm, bytes, in bw_seq:
#    print "%s: %d bytes, %d tuples" % (time.ctime(tm), bytes, tuples)
    time_data.append(get_x_from_time(tm))
    bw.append( 8 * bytes/ 1000 / 1000)

  MAX_Y = max(bw)
  
  ax.set_ylim( 0, 1.35 *  MAX_Y)  
  
  plt.tick_params(axis='both', which='major', labelsize=16)
  if DATE:
    bw_line, = ax.plot_date(time_data, bw, line_fmt, label="BW") 
  else:
    bw_line, = ax.plot(time_data, bw, line_fmt, label="BW") 
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel('Bandwidth (Mbits/sec)', fontsize=22)
  leg_artists.append( bw_line )


def  plot_src_tuples(time_to_count, ax, leg_artists):
#  print time_to_count
  
  counts = [y for (_,y) in time_to_count]
  times = [get_x_from_time(t) for (t,_) in time_to_count]
  MAX_Y = max(counts)
  
  ax.set_ylim( 0, 1.2 *  MAX_Y)  
  
  if DATE:
    line, = ax.plot_date(times,counts, "g.-") 
  else:
    line, = ax.plot(times,counts, "g.-") 


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
  time_data = [get_x_from_time(t) for t,l in level_transitions]
  lev_data = [l for t,l in level_transitions]
  if DATE:
    deg_line, = ax.plot_date(time_data, lev_data, line_fmt) 
  else:
    deg_line, = ax.plot(time_data, lev_data, line_fmt) 
#  ax.set_ylim( 0, 1.2 *  max(lev_data))    
  ax.set_ylim( 0, max(lev_data) * 2)  
 
  ax.set_ylabel(deg_label, fontsize=22)
  for tick in ax.yaxis.get_major_ticks():
    tick.label.set_fontsize(16) 
  ax.tick_params(axis='both', which='major', labelsize=16)
  leg_artists.append( deg_line )
#  print level_transitions  



    

def do_latency_bw_plot(time_to_bw, offset, options, min_time, max_time): 
    print "offset is ",offset, " min time ", min_time, "max_time ", max_time 
    lat_series = get_latencies_over_time(options.latency, offset, min_time, max_time)
    lat_series = smooth_seq(lat_series, 0, 60 * 60, window=10)
    print lat_series[0:10]
    
    figure, ax = plt.subplots()
    leg_artists = []

    #plot_bw(time_to_bw, ax, leg_artists, "b.-") 
    plot_latencies(lat_series, ax, leg_artists)
    plt.setp(leg_artists[-1], linewidth=2)
    
    if options.latency_no_degradation:
      lat_series_no_deg = get_latencies_over_time(options.latency_no_degradation, None, None, None)
      lat_series_no_deg = smooth_seq(lat_series_no_deg, 0, 60 * 60, window=10)
      plot_latencies(lat_series_no_deg, ax, leg_artists, "r--")
      plt.setp(leg_artists[-1], linewidth=2)
    
    finish_plots(figure, ax, leg_artists, options.outfile + "_latencies", ["With degradation", "Without degradation"])



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
  if offset == None:
    labels = [l/1000 for l, buckets in sorted(ret.items())]
    offset = min(labels)
    print "Calculating offset ", offset
  for l, buckets in sorted(ret.items()):
    t = l/1000 - offset
    if min_time != None and max_time != None:
      if t < min_time or t > max_time:
        continue
    q = quantile(buckets, sum(buckets.values()), QUANTILE) 
    ts_to_quant.append (   (t, q))
  
  return ts_to_quant
  
def plot_latencies(lat_series, old_ax, leg_artists, line_fmt="k-"):
  print "plotting latency data"
  ax = old_ax#old_ax.twinx()
  time_data = [get_x_from_time(t) for t,l in lat_series]
  latency_data = [l for t,l in lat_series]
  if DATE:
    line, = ax.plot_date(time_data, latency_data, line_fmt) 
  else:
    line, = ax.plot(time_data, latency_data, line_fmt) 
#  ax.set_ylim( 0, 1.2 *  max(lev_data))    
  ##ax.set_ylim( 0, max(latency_data) *1.2)  
  ax.set_ylim( 0, 5000)  
 
  ax.set_xlabel('Experiment time (sec)', fontsize=22)  
  ax.set_ylabel('Avg latency (msecs)', fontsize=22)
  leg_artists.append( line )
  
  
def get_src_bw(bw_seq, level_transitions, offset):
  res = []
  print "For source bw, have bw seq len %i and levels %i" % \
      (len(bw_seq), len(level_transitions))
  for (tm, b), (_, ratio) in zip(bw_seq, level_transitions):
    res.append( (tm,  b / ratio) )
  
  res = smooth_seq(res, 0, EXP_MINUTES * 60, window = 10)
  return res

def finish_plots(figure, ax, leg_artists, outname, label_strings, legend_loc=1):
  figure.subplots_adjust(left=0.15)
  figure.subplots_adjust(bottom=0.18)  
  figure.subplots_adjust(right=0.9)  
  #labels = ax.get_xticklabels() 
  #for label in labels: 
  #    label.set_rotation(30) 
      #"Src Records",   
  plt.legend(leg_artists, label_strings, loc=legend_loc, frameon=False);
  ax.tick_params(axis='both', which='major', labelsize=14) #controlls the x and left y
  plt.tick_params(axis='both', which='major', labelsize=14) #controlls the right y (twinx)
  
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
