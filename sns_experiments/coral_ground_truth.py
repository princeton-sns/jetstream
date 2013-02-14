from collections import defaultdict

import csv
import sys


def main():
  infile = sys.argv[1]
  print "reading data from %s" % infile
  
  response_times = defaultdict(int)
  sizes = defaultdict(int)

  with open(infile, 'r') as csvfile:
    reader = csv.reader(csvfile, skipinitialspace=True)
    for split_line in reader:
      update_hist(response_times, int(split_line[-3]))
      update_hist(sizes, int(split_line[-8]))
  
  print_hist(response_times, "distribution of response times in us")
  print_hist(sizes, "distribution of file sizes")



def  update_hist(response_times, t):
  p = 1
  while 2 * p < t:
    p *= 2
  t = p
  response_times[t] += 1
  return


def print_hist(hist, label):
  print "%s. %d distinct values" % (label, len(hist))
  if len(hist) < 15:
    for (k,v) in sorted(hist.items()):
      print "%d-%d %d" % (k, k * 2, v)
      
  return

if __name__ == '__main__':
    main()
    
    
    