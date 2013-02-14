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
  t = (t / 10) * 10
  response_times[t] += 1
  return


def print_hist(response_times, label):
  print label
  for (k,v) in sorted(response_times.items()):
    print "%d %d" % (k, v)
  return

if __name__ == '__main__':
    main()
    
    
    