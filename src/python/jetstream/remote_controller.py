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
  def __init__(self, netaddr=None):
    """ Set up a connection from a client to a controller process.

    netaddr should be a tuple in the following format: (host_IP, host_port)
    """
    self.node_cache = None

    # TODO make netaddr mandatory (why isn't it already?)
    if netaddr is not None:
      print "connect to :", netaddr
      self.connect(*netaddr)
  
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

  def deploy(self, op_graph, cube=None, cube_placement=None):
    """Deploys an operator graph"""
    if cube is not None:
      if cube_placement is not None:
          cube.instantiante_on(cube_placement)
      else:
          cube.instantiate_on(self.all_nodes())
    else:
      assert cube_placement is None

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
     
     
     
     
