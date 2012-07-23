

class OperatorGraph():

  def __init__(self):
    self.opID = 1
    self.edges = []

  def operator(self, desc):
    """Add an operator to the graph"""
    self.opID += 1
    o = Operator(self, opID)
    raise "Unimplemented!"
    
    
    
  def serialize(self):
    raise "Unimplemented"
    
    
  def connect(self, oper1, oper2):
    raise "Unimplemented"
    
    
    
class Operator():

  def __init__(self, graph, id):
    self.graph = graph  #keep link to parent OperatorGraph
    self.id = id
  
  def get_id():
    return self.id