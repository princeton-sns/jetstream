#TODO: This doesn't really correspond to the Controller implementation, since right now we are using
#handcrafted protobuf messages and process_message handlers in lieu of the traditional RPC stubs. Resolve.

class ControllerAPI():
  def all_nodes(self):
    """Returns a list of all nodes in the system."""
    raise "Unimplemented!"
    
  def get_a_node(self):
    """Picks a node for work assignment. Will round-robin around nodes"""
    raise "Unimplemented!"

  def deploy(self, op_graph):
    """Deploys an operator graph"""
    raise "Unimplemented!"
