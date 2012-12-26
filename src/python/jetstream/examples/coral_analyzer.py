from __future__ import print_function
from collections import deque

from client_reader import ClientDataReader#, tuple_str

import coral_parse

def main():
  cr = coral_parse.parse_setup()

  tuples = deque(maxlen=10000) # maximum size to bound memory usage

  def processdata(topk):
    assert len(topk) == 20, ' '.join([str(topk), "\nLength:", str(len(topk))])
  
    packet = list()
    for top in topk:
      entry = { 'domain' : top.e[0].s_val, 'count' : top.e[1].i_val}
      packet.append(entry)
  
    print(packet)
    if len(tuples) > 10: 
        import sys
        sys.exit()
    tuples.append({'data' : packet})
  
    #print tuple_str(tup)

  cr.blocking_read(processdata)

if __name__ == '__main__':
  main()
