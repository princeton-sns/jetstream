from collections import defaultdict
from optparse import OptionParser 

import csv
import resource
import sys
import time
import urlparse
import fileinput

RSS_units = "kbytes" if 'darwin' in sys.platform else 'mbytes'

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
    
  print "total of %d lines in input" % lines
  print "URL:"
  show_stats(lines, key_to_count)
  if not domains:
    key_to_count = remap_to_domain(key_to_count)
    print "Domain:"
    show_stats(lines, key_to_count)

  kb_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 - kb_used_start
  elapsed_time = time.time() - start_time
  print "\nAnalysis using %d %s in %d seconds" % (kb_used,RSS_units,elapsed_time)  

def to_timebucket(timestamp, time_bucket_size):
  if time_bucket_size == 0:
    return 0
  timestamp = (int(float(timestamp))  / time_bucket_size) * time_bucket_size
  return timestamp


def url2domain(url):
  u = urlparse.urlparse(url)
  return u.hostname if u.hostname else url
  
def remap_to_domain(key_to_count):
  r = defaultdict(int)
  for (kurl,ktime),v in key_to_count.iteritems():
    r[  (url2domain(kurl),ktime) ] += v
  return r
  
def show_stats(lines, key_to_count):
  recs_per_key = float(lines) / len(key_to_count)
  print "%d keys" %  len(key_to_count)
  frequencies = defaultdict(int)
  for count in key_to_count.values():
    frequencies[count] += 1
  print "%d elements in count-statistics" % len(frequencies)
  one_off_key_frac = frequencies[1] * 100.0 / len(key_to_count)
  one_off_vals = frequencies[1] * 100.0 / lines
  print "%d one-element keys (%0.2f%% of total keys, %0.2f%% of entries) and %0.2f recs per key" % \
    (frequencies[1], one_off_key_frac, one_off_vals, recs_per_key)
    

if __name__ == '__main__':
    main()
    
    
    