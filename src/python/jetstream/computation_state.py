#
# Classes representing the state of a JetStream controller, in particular the state of 
# a computation.
#

import time
import types

from jetstream_types_pb2 import *
from worker_assign import WorkerAssignment


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


  def get_dataplane_ep (self):
    return (self.lastHB.dataplane_addr.address, self.lastHB.dataplane_addr.portno)
  

  #TODO: unused after refactoring
  def create_assignment (self, compID):
    assert compID not in self.assignments
    self.assignments[compID] = WorkerAssignment(compID)
    return self.assignments[compID]


  def add_assignment (self, wa):
    assert wa.compID not in self.assignments
    self.assignments[wa.compID] = wa


class Computation (object):
  """Controller's view of a running computation"""
  
  def __init__ (self, compID):  #, jsGraph
    # Save the controller interface so we can communicate with workers
    self.compID = compID
    # Maps a worker endpoint to an assignment
    self.workerAssignments = {} # worker ID -> WorkerAssignment object


  def assign_worker (self, workerID, assignment):
    assert(workerID not in self.workerAssignments)
    self.workerAssignments[workerID] = assignment


  def update_worker (self, endpoint, actualAssignment):
    assert(endpoint in self.workerAssignments)
    intendedAssignment = self.workerAssignments[endpoint]
    # Currently, an assignment's state depends on whether it was fully realized
    if actualAssignment == intendedAssignment:
      intendedAssignment.state = WorkerAssignment.RUNNING
    else:
      intendedAssignment.state = WorkerAssignment.STOPPED
      #TODO Handle failed assignment here

  def workers_in_use(self):
    return [workerID for workerID, worker in self.workerAssignments.items() if len(worker.operators) > 0]
    

  #TODO: Move this code to controller.py once we incorporate computation stop logic
#   def stop (self):
#     # Stop each worker's assignment
#     #TODO: Do this in reverse topological order
#     for worker in self.workerAssignments.keys():
#       req = ControlMessage()
#       req.type = ControlMessage.ALTER
#       req.alter.computationID = self.compID
#       req.alter.taskToStop.extend( [operator.id for operator in self.workerAssignments[worker].operators] )
#       req.alter.cubesToStop.extend( [cube.name for cube in self.workerAssignments[worker].cubes] )
#       h = self.connect_to(worker)
#       h.send_pb(req)
