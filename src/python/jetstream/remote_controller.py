

from generic_netinterface import JSClient

from jetstream_types_pb2 import *

class RemoteController():

  def __init__(self):
    self.node_cache = None
  
  def connect(self, addr, port):
    self.client = JSClient( (addr, port) )

  def all_nodes(self):
    """Returns a list of all nodes in the system."""
    
    if self.node_cache is None:
      self.node_cache = []
      req = ControlMessage()
      req.type = ControlMessage.GET_NODE_LIST_REQ
  
      resp = self.client.ctrl_rpc(req, True)
      print resp

      for nID in resp.nodes:
        nID2 = NodeID()
        nID2.CopyFrom(nID)
        self.node_cache.append(  nID2 )
    return self.node_cache

  def get_a_node(self):
    if self.node_cache is None:
      self.all_nodes()
    return self.node_cache[0]
#    raise "Unimplemented!"

  def deploy(self, op_graph):
    """Deploys an operator graph"""
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    
    op_graph.add_to_PB(req.alter)
    print req
    resp = self.client.ctrl_rpc(req, True)
    print resp
    
