from collections import defaultdict

import csv
import sys
import time

def main():
  infile = sys.argv[1]
  print "reading data from %s" % infile
  
  hour_to_data= {}

  lines = 0
  with open(infile, 'r') as csvfile:
    reader = csv.reader(csvfile, skipinitialspace=True)
    for split_line in reader:
      hr = to_hour(split_line[1])
      if not hr in hour_to_data:
        response_times = defaultdict(int)
        sizes = defaultdict(int)
        hour_to_data[hr] = (response_times, sizes)
      else:
        (response_times, sizes) = hour_to_data[hr]
      update_hist(response_times, int(split_line[-3]))
      update_hist(sizes, int(split_line[-8]))
      lines += 1
  print "total of %d lines" % lines
  
  for hr,(response_times,sizes) in hour_to_data.items():
    print "For hour %s" % hr
    print_hist(response_times, "distribution of response times in us")
    print_hist(sizes, "distribution of file sizes")


def to_hour(timestamp):
  timestamp = (int(float(timestamp))  / 3600) * 3600
  s = time.ctime(timestamp)
  print s
  return s
  
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
    
    
    