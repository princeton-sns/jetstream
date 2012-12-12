


class SchemaError(Exception):
  def __init__(self, value):
    self.value = value

  def __str__(self):
    return repr(self.value)

# Enumeration of already-defined operators
class OpType (object):
  FILE_READ = "FileRead"
  STRING_GREP = "StringGrep"
  CSV_PARSE = "CSVParse"
  PARSE = "GenericParse"
  EXTEND = "ExtendOperator"
  TIMESTAMP = "TimestampOperator"
  T_ROUND_OPERATOR = "TRoundingOperator"
  VARIABLE_SAMPLING = "VariableSamplingOperator"
  CONGEST_CONTROL = "CongestionController"
  
  
  NO_OP = "ExtendOperator"  # ExtendOperator without config == NoOp
  SEND_K = "SendK"
  RATE_RECEIVER = "RateRecordReceiver"
  ECHO = "EchoOperator"
  RAND_SOURCE = "RandSourceOperator"
  RAND_EVAL = "RandEvalOperator"

  TIME_SUBSCRIBE = "TimeBasedSubscriber"
  LATENCY_MEASURE_SUBSCRIBER = "LatencyMeasureSubscriber"
  

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

def validate_parse(in_schema, cfg):
#  types':"DSS", 'field_to_parse
  field_to_parse = int( cfg['field_to_parse'])
  ret = []
  ret.extend( in_schema[0:field_to_parse] )
  for c in cfg['types']:
    ret.append ( (c, ''))
  ret.extend( in_schema[field_to_parse+1:] )
  return ret

def validate_extend(in_schema, cfg):
#  print "extend with cfg",cfg
  newS = []
  newS.extend(in_schema)
  for x in cfg['types']:
    newS.append( (x.upper(), '') )
  return newS

def validate_timestamp(in_schema, cfg):
  newS = []
  newS.extend(in_schema)
  if cfg["type"]=="s":
    newS.append( ("T", 'timestamp(s)') )
  if cfg["type"]=="ms":
    newS.append( ("D", 'timestamp(ms)') )
  if cfg["type"]=="us":
    newS.append( ("D", 'timestamp(us)') )
  return newS

def validate_latency_measure(in_schema, cfg):
  newS = []
  newS.append( ("S", 'latency measure') )
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

def validate_CSVParse(in_schema, cfg):
  if in_schema[0][0] != 'S':
    raise SchemaError("CSVParse currently requires a string as the first" +
                       "element of an input tuple")

  valid_types = 'SDI'
  if any(t not in valid_types for t in cfg['types']):
    raise SchemaError("CSVParse currently only accepts string, double, and" +
                      "32-bit integer types")

  return [(t, '') for t in cfg['types']]

  
# Schemas are represented as a function that maps from an input schema and configuration
# to an output schema
# A schema itself is a list of pairs, where the first element is a typecode [I,D,S, or T]
# and the second field is a name for that element.
SCHEMAS = {} 
SCHEMAS[OpType.FILE_READ] = validate_FileRead
SCHEMAS[OpType.STRING_GREP] = validate_grep
SCHEMAS[OpType.PARSE] = validate_parse
SCHEMAS[OpType.EXTEND] = validate_extend
SCHEMAS[OpType.TIMESTAMP] = validate_timestamp
SCHEMAS[OpType.LATENCY_MEASURE_SUBSCRIBER] = validate_latency_measure
SCHEMAS[OpType.T_ROUND_OPERATOR] = validate_TRound

SCHEMAS[OpType.VARIABLE_SAMPLING] = lambda schema,cfg: schema 
SCHEMAS[OpType.CONGEST_CONTROL] = lambda schema,cfg: schema 


SCHEMAS[OpType.ECHO] = lambda schema,cfg: schema 
SCHEMAS[OpType.SEND_K] =  lambda schema,cfg: [('I','K')]
SCHEMAS[OpType.RATE_RECEIVER] = lambda schema,cfg: schema
SCHEMAS[OpType.RAND_SOURCE] = lambda schema,cfg: [('S','state'), ('T', 'timestamp')]
SCHEMAS[OpType.RAND_EVAL] = validate_RandEval
# TODO RAND_EVAL
#  SCHEMAS[NO_OP] = lambda x: x

SCHEMAS[OpType.UNIX] =  lambda schema,cfg: [("S","")]

SCHEMAS[OpType.CSV_PARSE] = validate_CSVParse

