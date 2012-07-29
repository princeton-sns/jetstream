
class ControllerAPI():
  
  # Client-facing API
  def deploy(self, op_graph):
    """Deploys an operator graph"""
    raise "Unimplemented!"

  def kill(self, op_graph_name):
    """Kills an operator graph"""
    raise "Unimplemented!"

  # Worker-facing API
  #SS: Should this be a NodeID instead (i.e. controller is agnostic to worker threads)?
  def register(workerID):
    """Registers a worker with the controller."""
    raise "Unimplemented!"
