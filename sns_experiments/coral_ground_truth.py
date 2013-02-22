from collections import defaultdict
from optparse import OptionParser 

import csv
import sys
import time

def main():
  parser = OptionParser()

  parser.add_option("-t", "--timewindow", dest="timewindow", 
                  help="time window", default="10")
  
  (options, args) = parser.parse_args()
  infile = args[0]
  print "reading data from %s" % infile
  
  time_bucket_size = int( options.timewindow)
  time_to_data= {}

  lines,parse_errs = 0,0
  with open(infile, 'r') as csvfile:
    reader = csv.reader(csvfile, skipinitialspace=True)
    
    for split_line in reader:
      try:
        time_bucket = to_timebucket(split_line[1], time_bucket_size)
        if not time_bucket in time_to_data:
          response_times, sizes, response_codes = [],[], defaultdict(int)
  #        response_times = defaultdict(int)
  #        sizes = defaultdict(int)
          time_to_data[time_bucket] = (response_times, sizes, response_codes)
        else:
          (response_times, sizes, response_codes) = time_to_data[time_bucket]
        
        update_exact(response_times, int(split_line[-3]))
        update_exact(sizes, int(split_line[-8]))
        response_codes[ int(split_line[-9]) ] += 1
        lines += 1
      except ValueError as e:
        parse_errs += 1
  print "total of %d lines, %d parse errors" % (lines, parse_errs)
  
  for t,(response_times,sizes, response_codes) in sorted(time_to_data.items()):
    total_in_time = len(response_times)
    print "For time %s, %d items:" % (time.ctime(t), total_in_time)
    print_exact(response_times, "distribution of response times in us")
    print_exact(sizes, "distribution of file sizes")
    print ", ".join(["%s: %0.2f%%" % (code, 100.0 * count/total_in_time) for (code,count) in sorted(response_codes.items())])
    print ""

def to_timebucket(timestamp, time_bucket_size):
  timestamp = (int(float(timestamp))  / time_bucket_size) * time_bucket_size
  return timestamp
  

def update_exact(response_times, val):
    response_times.append(val)
  
QUANTS = [0.05, 0.1, 0.5, 0.9, 0.95]
def print_exact(a_list, label):
   a_list.sort()
   quants = [str(a_list[ int(q * len(a_list))]) for q in QUANTS]
   q_str = " - ".join( quants )
   print "%s:quantiles are %s" % (label, q_str)
  
def  update_hist(response_times, t):
  p = 1
  while 2 * p < t:
    p *= 2
  t = p
  response_times[t] += 1
  return


def print_hist(hist, label):
  print "\t%s. %d distinct values" % (label, len(hist))
  if len(hist) < 30:
    for (k,v) in sorted(hist.items()):
      print "\t\t%d-%d %d" % (k, k * 2, v)
  return

if __name__ == '__main__':
    main()
    
    
    