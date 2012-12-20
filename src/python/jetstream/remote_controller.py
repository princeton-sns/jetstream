from generic_netinterface import JSClient

from jetstream_types_pb2 import *


def normalize_controller_addr(addr_str):
  if ':' in addr_str:
    (serv_addr, serv_port) = addr_str.split(':')
    serv_port = int(serv_port)
  else:
    serv_addr = options.controller
    serv_port = 3456  #default
    
  return   serv_addr, serv_port 


class RemoteController():
  def __init__(self, netaddr=None, cube_spec=None):
    """ If cube_spec is not None, then its first element should be a Cube, and its
    second element should be a set of nodes to place it on, or None. If the
    second element is none, then the cube is instantiated on all of the
    controller's nodes. """
    self.node_cache = None

    if netaddr is not None:
      print "connect to :", netaddr
      self.connect(*netaddr)

      # place cube
      if cube_spec is not None:
        cube = cube_spec[0]

        cube_placement = cube_spec[1]
        if cube_placement is None:
          cube_placement = self.all_nodes()

        cube.instantiate_on(cube_placement)

    else:
      # TODO make netaddr mandatory (why isn't it already?) (and remove the
      # connect method) and we won't have to worry about this case
      assert cube_spec is None
  
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
    resp = self.client.ctrl_rpc(op_graph.get_deploy_pb(), True)
    print resp
    
  def deploy_pb(self, req):
    """Deploys an operator graph; returns an integer ID or an error message"""
    resp = self.client.ctrl_rpc(req, True)
    if resp.type == ControlMessage.OK:
      return resp.started_comp_id
    else:
      return resp.error_msg.msg
 
  def stop_computation(self, comput_id):
    req = ControlMessage()
    req.type = ControlMessage.STOP_COMPUTATION
    req.comp_to_stop = int(comput_id)
    resp = self.client.ctrl_rpc(req, True)
    return resp  
     
     
     
     
