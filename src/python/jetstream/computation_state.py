#
# Classes representing a JetStream controller's state, in particular the state of 
# a computation.
#

import time

from jetstream_types_pb2 import *


class WorkerAssignment (object):
  """An assignment of operators/cubes to a worker for a given computation"""
  
  # States of an assignment
  RUNNING = 1
  STOPPED = 2
  
  def __init__ (self, compID):
    self.compID = compID
    self.state = WorkerAssignment.STOPPED
    self.operators = []
    self.cubes = []


class CWorker (object):
  """Controller's view of a worker node"""

  # States of a worker node
  ALIVE = 1
  DEAD = 2

  DEFAULT_HB_INTERVAL_SECS = 2
  # Number of heartbeat intervals of silence before declared dead
  DEFAULT_HB_DEAD_INTERVALS = 3
  
  def __init__ (self, endpoint, hbInterval=DEFAULT_HB_INTERVAL_SECS,
               hbDeadIntervals=DEFAULT_HB_DEAD_INTERVALS):
    self.endpoint = endpoint
    self.hbInterval = hbInterval
    self.hbDeadIntervals = hbDeadIntervals
    self.state = CWorker.DEAD
    self.lastHeard = 0
    self.lastHB = None
    # Maps a computation ID to an assignment
    self.assignments = {}


  def receive_hb (self, hb):
    self.lastHB = hb
    self.lastHeard = long(time.time())
    self.state = CWorker.ALIVE


  def update_state (self):
    t = long(time.time())
    if t - self.lastHeard > long(self.hbInterval * self.hbDeadIntervals):
      self.state = CWorker.DEAD
    return self.state


  def create_assignment (self, compID):
    self.assignments[compID] = WorkerAssignment(compID)
    return self.assignments[compID]
    print "DONE CREATING ASS"


class Computation (object):
  """Controller's view of a computation"""

  def __init__ (self, controller, compID, opGraph=None):
    # Save the controller interface so we can communicate with workers
    self.controller = controller
    self.computationID = compID
    self.opGraph = opGraph
    # Maps a worker endpoint to an assignment
    self.workerAssignments = {}


  def assign_worker (self, endpoint, assignment):
    assert(endpoint not in self.workerAssignments)
    self.workerAssignments[endpoint] = assignment


  def start (self):
    # Start each worker's assignment
    for worker in self.workerAssignments.keys():
      req = ControlMessage()
      req.type = ControlMessage.ALTER
      req.alter.toStart.extend(self.workerAssignments[worker].operators)
      req.alter.toCreate.extend(self.workerAssignments[worker].cubes)
      h = self.controller.connect_to(worker)
      h.send_pb(req)


  def stop (self):
    # Stop each worker's assignment
    #TODO: Do this in reverse topological order
    for worker in self.workerAssignments.keys():
      req = ControlMessage()
      req.type = ControlMessage.ALTER
      req.alter.taskToStop.extend( [operator.id for operator in self.workerAssignments[worker].operators] )
      req.alter.cubesToStop.extend( [cube.name for cube in self.workerAssignments[worker].cubes] )
      h = self.connect_to(worker)
      h.send_pb(req)

  def pause ():
    pass
