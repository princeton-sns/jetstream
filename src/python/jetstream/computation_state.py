#
# Classes representing a JetStream controller's state, in particular the state of 
# a computation.
#

import time

from jetstream_types_pb2 import *


class WorkerState(object):
  """Worker state stored by the controller"""

  # States
  ALIVE = 1
  DEAD = 2

  DEFAULT_HB_INTERVAL_SECS = 2
  # Number of heartbeat intervals of silence before declared dead
  DEFAULT_HB_DEAD_INTERVALS = 3
  
  def __init__(self, endpoint, hbInterval=DEFAULT_HB_INTERVAL_SECS,
               hbDeadIntervals=DEFAULT_HB_DEAD_INTERVALS):
    self.endpoint = endpoint
    self.hbInterval = hbInterval
    self.hbDeadIntervals = hbDeadIntervals
    self.state = WorkerState.DEAD
    self.lastHeard = 0
    self.lastHB = None
    # A worker runs (parts of) one or more computations
    self.computations = []


  def receive_hb(self, hb):
    self.lastHB = hb
    self.lastHeard = long(time.time())
    self.state = WorkerState.ALIVE


  def update_state(self):
    t = long(time.time())
    if t - self.lastHeard > long(self.hbInterval * self.hbDeadIntervals):
      self.state = WorkerState.DEAD
    return self.state
