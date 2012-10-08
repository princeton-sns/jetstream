#
# Classes representing the state of a JetStream controller, in particular the state of 
# a computation.
#

import time
import types

from jetstream_types_pb2 import *


class WorkerAssignment (object):
  """An assignment of operators/cubes to a worker for a given computation"""
  
  # States of an assignment
  RUNNING = 1
  STOPPED = 2
  
  def __init__ (self, compID, operators=None, cubes=None):
    self.compID = compID
    self.state = WorkerAssignment.STOPPED
      #lists of protobuf items
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


  def add_node (self, node):
    if isinstance(node, TaskMeta):
      self.operators.append(node)
    else:
      assert(isinstance(node, CubeMeta))
      self.cubes.append(node)


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
  """Controller's view of a running computation"""
  
  def __init__ (self, compID, jsGraph):
    # Save the controller interface so we can communicate with workers
    self.compID = compID
    self.jsGraph = jsGraph
    # Maps a worker endpoint to an assignment
    self.workerAssignments = {} #worker ID to WorkerAssignment object
    self.taskLocations = {} #maps operator ID or cube name to host,port pair
          # operator IDs are just ints for now.
    self.outEdges = {} #map from operator ID to destination.
          # right now dest is just an operator ID but might become an optional list.


  def assign_worker (self, endpoint, assignment):
    assert(endpoint not in self.workerAssignments)
    for task in assignment.operators:
      self.taskLocations[task.id.task] = endpoint
    for cube in assignment.cubes:
      self.taskLocations[str(cube.name)] = endpoint
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
      

  def add_edges(self, edgeList):
    print "adding",len(edgeList),"edges"
    for edge in edgeList:
      if edge.src not in self.taskLocations:
        print "unknown source %s" % str(edge.src)
        raise UserException("Edge from nonexistent source")
      dest = edge.dest if edge.HasField("dest") else str(edge.cube_name)
      self.outEdges[edge.src] = dest


  def get_worker_pb(self, workerID):
    """Returns the control message to start the portion of this computation on worker 
    with id workerID"""
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    req.alter.computationID = self.compID
    req.alter.toStart.extend(self.workerAssignments[workerID].operators)
    req.alter.toCreate.extend(self.workerAssignments[workerID].cubes)
    
    print self.taskLocations
      #now the edges
    for operator in self.workerAssignments[workerID].operators:
      tid = operator.id.task
      if tid in self.outEdges: #operator has a link to next
        destID = self.outEdges[tid]
        pb_e = req.alter.edges.add()
        pb_e.src = tid
        pb_e.computation = self.compID
        
        dest_host = self.taskLocations[destID]

        if type(destID) == types.StringType:
          pb_e.cube_name = destID
        elif type(destID) == types.IntType:
          pb_e.dest = destID
        else:
          print "no such task: %s of type %s" % (str(destID), str(type(destID)))
          assert False           

        if dest_host != workerID:
          pb_e.dest_addr.address = dest_host[0]
          pb_e.dest_addr.portno = dest_host[1]
          
    return req


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
