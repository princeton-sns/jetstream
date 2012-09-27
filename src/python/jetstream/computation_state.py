#
# Classes representing the state of a JetStream controller, in particular the state of 
# a computation.
#

import time

from jetstream_types_pb2 import *


class WorkerAssignment (object):
  """An assignment of operators/cubes to a worker for a given computation"""
  
  # States of an assignment
  RUNNING = 1
  STOPPED = 2
  
  def __init__ (self, compID, operators=None, cubes=None):
    self.compID = compID
    self.state = WorkerAssignment.STOPPED
    self.operators = operators if operators is not None else []
    self.cubes = cubes if cubes is not None else []


  def __eq__ (self, other):
    if isinstance(other, WorkerAssignment):
      if ((len(self.operators) != len(other.operators)) or
          (len(self.cubes) != len(other.cubes)) or
          (self.compID != other.compID)):
        return False
      # Compare the list of operators and cubes (assume they are sorted for now)
      for i in range(len(self.operators)):
        if self.operators[i].id.task != other.operators[i].id.task:
          return False
      for i in range(len(self.cubes)):
        if self.cubes[i].name != other.cubes[i].name:
          return False
      return True
    return NotImplemented


  def __ne__ (self, other):
    result = self.__eq__(other)
    if result is NotImplemented:
      return result
    return not result


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
    assert compID not in self.assignments
    self.assignments[compID] = WorkerAssignment(compID)
    return self.assignments[compID]


class Computation (object):
  """Controller's view of a computation"""

  def __init__ (self, controller, compID, opGraph=None):
    # Save the controller interface so we can communicate with workers
    self.controller = controller
    self.compID = compID
    self.opGraph = opGraph
    # Maps a worker endpoint to an assignment
    self.workerAssignments = {}


  def assign_worker (self, endpoint, assignment):
    assert(endpoint not in self.workerAssignments)
    self.workerAssignments[endpoint] = assignment


  def update_worker (self, endpoint, actualAssignment):
    assert(endpoint in self.workerAssignments)
    intendedAssignment = self.workerAssignments[endpoint]
    # Currently, an assignment's state depends on whether it was fully realized
    if actualAssignment == intendedAssignment:
      intendedAssignment.state = WorkerAssignment.RUNNING
    else:
      intendedAssignment.state = WorkerAssignment.STOPPED
      #TODO Handle failed assignment here
      

  def start (self):
    # Start each worker's assignment
    for worker in self.workerAssignments.keys():
      req = ControlMessage()
      req.type = ControlMessage.ALTER
      req.alter.computationID = self.compID
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
      req.alter.computationID = self.compID
      req.alter.taskToStop.extend( [operator.id for operator in self.workerAssignments[worker].operators] )
      req.alter.cubesToStop.extend( [cube.name for cube in self.workerAssignments[worker].cubes] )
      h = self.connect_to(worker)
      h.send_pb(req)
