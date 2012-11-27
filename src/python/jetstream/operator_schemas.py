


class SchemaError(Exception):
  def __init__(self, value):
    self.value = value

  def __str__(self):
    return repr(self.value)

# Enumeration of already-defined operators
class OpType (object):
  FILE_READ = "FileRead"
  STRING_GREP = "StringGrep"
  PARSE = "GenericParse"
  EXTEND = "ExtendOperator"
  T_ROUND_OPERATOR = "TRoundingOperator"
  
  NO_OP = "ExtendOperator"  # ExtendOperator without config == NoOp
  SEND_K = "SendK"
  RATE_RECEIVER = "RateRecordReceiver"
  RAND_SOURCE = "RandSourceOperator"
  RAND_EVAL = "RandEvalOperator"

  TIME_SUBSCRIBE = "TimeBasedSubscriber"
  

  # Supported by Python local controller/worker only
  UNIX = "Unix"
  FETCHER = "Fetcher"



def validate_FileRead(in_schema, cfg):
  if len(in_schema) > 0:
    raise SchemaError("File Read should take no inputs, but got %s" % str(in_schema))
  return [("S","")]

def validate_grep(in_schema, cfg):
  fld = cfg['id']
  if fld > len(in_schema) or in_schema[fld][0] != 'S':
    raise SchemaError("Can't grep on field %d of %s" % (fld, str(in_schema)))
  return in_schema

def validate_Extend(in_schema, cfg):
#  print "extend with cfg",cfg
  newS = []
  newS.extend(in_schema)
  for x in cfg['types']:
    newS.append( (x.upper(), '') )
  return newS

def validate_TRound(in_schema, cfg):
  fld_offset = int(cfg['fld_offset'])
  if fld_offset >= len(in_schema):
    raise SchemaError("can't round field %d since input only has %d fields (%s)." % \
         (fld_offset, len(in_schema), str(in_schema)))
    
  t = in_schema[fld_offset][0]
  if t != "T":
    raise SchemaError("rounding operator requires that field %d be a time, instead was %s" % (fld_offset,t))
  return in_schema

def validate_RandEval(in_schema, cfg):
  in_types = [ty for ty,name in in_schema[0:3]]
  if in_types != ['S','T', 'I']:
    raise SchemaError("rand eval requires inputs Str, Time, Int. Got %s" % str(in_schema))
    
  return []    
  
SCHEMAS = {}
SCHEMAS[OpType.FILE_READ] = validate_FileRead
SCHEMAS[OpType.STRING_GREP] = validate_grep
# TODO PARSE
SCHEMAS[OpType.EXTEND] = validate_Extend
SCHEMAS[OpType.T_ROUND_OPERATOR] = validate_TRound

SCHEMAS[OpType.SEND_K] =  lambda schema,cfg: [('I','K')]
SCHEMAS[OpType.RATE_RECEIVER] = lambda schema,cfg: schema
SCHEMAS[OpType.RAND_SOURCE] = lambda schema,cfg: [('S','state'), ('T', 'timestamp')]
SCHEMAS[OpType.RAND_EVAL] = validate_RandEval
# TODO RAND_EVAL
#  SCHEMAS[NO_OP] = lambda x: x



