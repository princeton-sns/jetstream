from collections import defaultdict
from optparse import OptionParser 

import csv
import sys
import time

def main():


  parser = OptionParser()
  parser.add_option("-o", "--out-file", dest="filename", action="store",help="generate data at union node", default="data.out")


  (options, args) = parser.parse_args()

  infile = args[0]
  if args[1] == 'size':
    col = -8
  elif args[1] == 'time':
    col = -3
  else:
    col = int (args[1])  
  print "reading data from %s; printing col %d to %s" % (infile, col, options.filename)
  f = open(options.filename, 'w')
  lines = 0
  with open(infile, 'r') as csvfile:
    reader = csv.reader(csvfile, skipinitialspace=True)
    for split_line in reader:
      v = int(split_line[col])
      print >>f, v
      lines += 1
  print "total of %d lines" % lines
  f.close()
  
  
    
if __name__ == '__main__':
    main()

  