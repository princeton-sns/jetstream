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
  PROBATION = 2
  DEAD = 3

  DEFAULT_HB_INTERVAL_SECS = 2
  # Number of consecutive heartbeats before declared alive
  DEFAULT_HB_ALIVE_COUNT = 3
  # Number of heartbeat intervals of silence before declared dead
  DEFAULT_HB_DEAD_INTERVALS = 3
  
  def __init__(self, address, hbInterval=DEFAULT_HB_INTERVAL_SECS,
               hbAliveCount=DEFAULT_HB_ALIVE_COUNT,
               hbDeadIntervals=DEFAULT_HB_DEAD_INTERVALS):
    self.address = address
    self.hbInterval = hbInterval
    self.hbAliveCount = hbAliveCount
    self.hbDeadIntervals = hbDeadIntervals
    self.state = WorkerState.DEAD
    self.probationCount = 0
    self.lastHeard = 0
    self.lastHB = None
    # A worker runs (parts of) one or more computations
    self.computations = []


  def receive_hb(self, hb):
    self.lastHB = hb
    self.lastHeard = long(time.time())
    if self.state == WorkerState.DEAD:
      self.state = WorkerState.PROBATION
      self.probationCount = 1
    elif self.state == WorkerState.PROBATION:
      self.probationCount += 1
      if self.probationCount == self.hbAliveCount:
        self.state == WorkerState.ALIVE


  def update_state(self):
    t = long(time.time())
    if t - self.lastHeard > long(self.hbInterval * self.hbDeadIntervals):
      self.state = WorkerState.DEAD
    return self.state
