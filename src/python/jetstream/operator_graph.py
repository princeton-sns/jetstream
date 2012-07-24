
#Enumeration of already-defined operators
class Operators(object):

  UNIX = "Unix"
  Fetcher = "Fetcher"

class OperatorGraph(object):

  def __init__(self):
    self.opID = 1
    self.edges = set([])
    self.operators = []
    self.cubes = []


  def operator(self, type, desc):
    """Add an operator to the graph"""
    self.opID += 1
    o = Operator(self, type, desc, self.opID)
    self.operators.append(o)
    return o
    
    
  def cube(self, name, desc):
    """Add an operator to the graph"""
    self.opID += 1
    o = Cube(self, name, desc, self.opID)
    self.cubes.append(o)
    return o
    
    
  def serialize(self):
    raise "Unimplemented"
    
    
  def connect(self, oper1, oper2):
    self.edges.add( (oper1.get_id(), oper2.get_id()) )
    oper2.add_pred(oper1)
    
    
    
## These represent the abstract concept of an operator or cube, for building
# the operator graphs. The concrete executable implementations are elsewhere.
class Destination(object):
  
  def __init__(self, graph, id):
    self.preds = set()
    self.graph = graph  #keep link to parent OperatorGraph
    self.id = id
    self._location = None

  
  def add_pred(self, p):
    self.preds.add(p)
  
  def get_id(self):
    return self.id
    
  def location(self):
    return self._location
    
  def is_placed(self):
    return self._location is not None    

  def instantiate_on(self, n):
    self._location = n
    for p in self.preds:
      if not p.is_placed():
        p.instantiate_on(n)

class Operator(Destination):
  
  def __init__(self, graph, type, desc, id):
    super(Operator,self).__init__(graph, id)
    self.type = type
    self.desc = desc
    

class Cube(Destination):

  def __init__(self, graph, name, desc, id):
    super(Cube,self).__init__(graph, id)
    self.name = name
    self.desc = desc

     
  def get_name(self):
    if self._location is not None:
      return  "%s:%d/%s"% (self._location.address, self._location.portno, self.name)
    else:
      return self.name

