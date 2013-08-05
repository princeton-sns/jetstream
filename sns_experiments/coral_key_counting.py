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
  parser.add_option("-n", "--noargs", dest="noargs", action="store_true", default=False,
                  help="whether to strip arguments from URLs")
    
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
    if options.noargs:
      url = strip_args(url)
    key = (url, time_bucket)
    key_to_count[key] +=1
    lines += 1
    
  print "total of %d lines in input" % lines
#  print "URL:"
  stats = {}
  show_stats(lines, key_to_count)
  for t in [5,60, 300, 60 * 60, 0]:
    key_to_count = fold_time(key_to_count, t)
    print "\n\nUsing %d second buckets:" % t
    urlstats = show_stats(lines, key_to_count)
    domstats = show_stats(lines, remap_to_domain(key_to_count))
    stats[t] = (urlstats, domstats)
#    key_to_count = remap_to_domain(key_to_count)
#    print "Domain:"
#    show_stats(lines, key_to_count)

  kb_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 - kb_used_start
  elapsed_time = time.time() - start_time
  print "\nAnalysis using %d %s in %d seconds" % (kb_used,RSS_units,elapsed_time)  
  print_stats(stats)

def to_timebucket(timestamp, time_bucket_size):
  if time_bucket_size == 0:
    return 0
  timestamp = (int(float(timestamp))  / time_bucket_size) * time_bucket_size
  return timestamp


def url2domain(url):
  u = urlparse.urlparse(url)
  return u.hostname if u.hostname else url
  
def strip_args(url):
  u = urlparse.urlparse(url)
  return u.hostname + u.path if u.hostname else url
  
  
def remap_to_domain(key_to_count):
  r = defaultdict(int)
  for (kurl,ktime),v in key_to_count.iteritems():
    r[  (url2domain(kurl),ktime) ] += v
  return r
  
  
def fold_time(key_to_count, t):
  r = defaultdict(int)
  for (kurl,ktime),v in key_to_count.iteritems():
    r[  (kurl,to_timebucket(ktime, t)) ] += v
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
  return recs_per_key

def print_stats(stats):
  stats[24 * 60 * 60] = stats[0]
  del stats[0]
  print "\n\n\nTime & URL occurrences & Dom occurrences"
  for t, (url, dom) in sorted( stats.items()):
    print "%d %0.2f %0.2f" % (t, url, dom)
    

if __name__ == '__main__':
    main()
    
    
    