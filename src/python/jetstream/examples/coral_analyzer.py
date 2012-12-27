from collections import deque
import sys
import functools

from client_reader import ClientDataReader#, tuple_str

import coral_parse

class CoralTopKCollector(object):
  def __init__(self, memory_length=1000):
    self.tuples = deque(maxlen=memory_length)
    self.cr = coral_parse.parse_setup()

    print self.reformat_topk_and_store
    import sys; sys.exit

  def reformat_topk_and_store(self, topk):
    #assert len(topk) == 20, ' '.join([str(topk), "\nLength:", str(len(topk))])
    if len(topk) != 20:
      print 'WARNING: len not 20: %d' % len(topk)

    packet = [{'domain':top.e[0].s_val,'count':top.e[1].i_val} for top in topk]
    packet = {'data' : packet}
    self.tuples.append(packet)

    #print packet

    # FIXME TODO REMOVEME
    #if len(self.tuples) > 10: 
      #sys.exit()

    return packet

  def __call__(self, f=None):
    if f is not None:
      self.cr.blocking_read(f)
    else:
      self.cr.blocking_read(self.reformat_topk_and_store)


def main():
  CoralTopKCollector()()

if __name__ == '__main__':
  main()
