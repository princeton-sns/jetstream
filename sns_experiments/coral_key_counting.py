from collections import defaultdict
from optparse import OptionParser 

import csv
import resource
import sys
import time
import urlparse
import fileinput


def main():
  parser = OptionParser()

  parser.add_option("-t", "--timewindow", dest="timewindow", 
                  help="time window", default="1")
  parser.add_option("-d", "--domains", dest="domains", action="store_true", default=False,
                  help="whether to use domains instead of full URLs")
    
  (options, args) = parser.parse_args()
  time_bucket_size = int( options.timewindow)
  domains = options.domains
  
  kb_used_start = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
  start_time = time.time()
  key_to_count = defaultdict(int)

  lines = 0
  input = fileinput.input(args)
  reader = csv.reader(input, skipinitialspace=True)    
  for split_line in reader:
    time_bucket = to_timebucket(split_line[1], time_bucket_size)
    url = split_line[6]
    if domains:
      url = url2domain(url) 
    key = (url, time_bucket)
    key_to_count[key] +=1
    lines += 1
  recs_per_key = float(lines) / len(key_to_count)
  print "total of %d lines and %d keys" %  (lines, len(key_to_count),  )
  frequencies = defaultdict(int)
  for count in key_to_count.values():
    frequencies[count] += 1
  print "%d unique keys (%0.2f%% of total keys) and %0.2f recs per key" % \
    (frequencies[1], frequencies[1] * 100.0 / len(key_to_count), recs_per_key)
    
  kb_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 - kb_used_start
  elapsed_time = time.time() - start_time
  print "\nAnalysis using %d kbytes in %d seconds" % (kb_used,elapsed_time)

def to_timebucket(timestamp, time_bucket_size):
  if time_bucket_size == 0:
    return 0
  timestamp = (int(float(timestamp))  / time_bucket_size) * time_bucket_size
  return timestamp


def url2domain(url):
  u = urlparse.urlparse(url)
  return u.hostname if u.hostname else url

if __name__ == '__main__':
    main()
    
    
    