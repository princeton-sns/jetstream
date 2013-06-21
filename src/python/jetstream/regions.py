

import re
import sys

def get_1_from_region(region_re, nodelist):
  for n in nodelist:
    if region_re.match(n[0]):
      return n