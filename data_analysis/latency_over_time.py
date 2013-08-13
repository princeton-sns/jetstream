from collections import defaultdict
import re
import sys
import numpy
import numpy.linalg
from numpy import array



OUT_TO_FILE = True

import matplotlib
if OUT_TO_FILE:
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

#    matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True



def main():
  infile = sys.argv[1]
  
  data = parse_infile(infile)
  plot_data_over_time(data)



def parse_infile(infile):
  ret = []
  f = open(infile, 'r')
  for ln in f:

  f.close()
  return ret


if __name__ == '__main__':
  main()
