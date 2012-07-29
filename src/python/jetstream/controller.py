import math

from controller_api import ControllerAPI
#from jetstream.gen.jetstream_types_pb2 import *

from operator_graph import *

class Controller(ControllerAPI):
  """Represents a stand-alone controller."""

  def __init__(self, netInterface):
    self.netInterface = netInterface
    self.workers = set()

  #def get_nodes(self):
  #  return []
    
  def deploy(self, op_graph):
    """Deploys an operator graph"""

    # Optimize the graph. When this returns, each operator/cube represents a single task.
    self.optimize(op_graph)

    #FIXME: Where should we create taskIds?
    workerToTasks = {}
    taskIdsToWorker = {}
    # Inane equipartition assignment for now.
    # Assign operators
    workerIndex = 0
    #FIXME: Construct AlterTopo message here instead? Currently using [[toStart],[edges]]
    workerToTasks[self.workers[workerIndex]] = [[],[]]
    tasksPerWorker = (int)(ceil(len(op_graph.operators) / len(self.workers)))
    i = 0
    for op in op_graph.operators:
      if i < tasksPerWorker:
        workerToTasks[self.workers[workerIndex]][0].append(op)
        taskIdsToWorker[op.get_id()] = [op, self.workers[workerIndex]]
        i += 1
      else:
        i = 0
        workerIndex += 1
        workerToTasks[self.workers[workerIndex]] = []
    # Assign cubes
    workerIndex = 0
    tasksPerWorker = (int)(ceil(len(op_graph.cubes) / len(self.workers)))
    i = 0
    for cube in op_graph.cubes:
      if i < tasksPerWorker:
        workerToTasks[self.workers[workerIndex]][0].append(cube)
        taskIdsToWorker[cube.get_id()] = [cube, self.workers[workerIndex]]
        i += 1
      else:
        i = 0
        workerIndex += 1
        if self.workers[workerIndex] not in workerToTasks:
          workerToTasks[self.workers[workerIndex]] = [[],[]]
    # Assign edges
    for (e1, e2) in op_graph.edges:
      workerToTasks[taskIdsToWorker[e1][1]][1].append(taskIdsToWorker[e2][0])
    
    # Make RPC calls to worker nodes here...

    #for op in dests.values():
    #  if isinstance(op, Operator):
    #    op.start()

  def optimize(op_graph):
    return

  # Worker-facing API
  def register(workerId):
    # This should eventually be a dictionary mapping workerId to worker resources.
    if workerId not in self.workers:
      self.workers.add(workerId)

def main():
  # Local testing/sanity checking.
  pass

if __name__ == '__main__':
  main()

