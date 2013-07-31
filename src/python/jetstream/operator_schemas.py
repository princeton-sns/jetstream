


class SchemaError(Exception):
  def __init__(self, value):
    self.value = value

  def __str__(self):
    return repr(self.value)

# Enumeration of already-defined operators
class OpType (object):
  FILE_READ = "CFileRead"
  STRING_GREP = "StringGrep"
  CSV_PARSE = "CSVParseStrTk"
  PARSE = "GenericParse"
  EXTEND = "ExtendOperator"
  TIMESTAMP = "TimestampOperator"
  T_ROUND_OPERATOR = "TRoundingOperator"
  VARIABLE_SAMPLING = "VariableSamplingOperator"
  CONGEST_CONTROL = "CongestionController"
  QUANTILE = "QuantileOperator"
  TO_SUMMARY = "ToSummary"
  SUMMARY_TO_COUNT = "SummaryToCount"
  DEGRADE_SUMMARY = "DegradeSummary"
  PROJECT = "ProjectionOperator"
  URLToDomain = "URLToDomain"
  COUNT_LOGGER = "CountLogger"
  EQUALS_FILTER = "IEqualityFilter"
  GT_FILTER = "GreaterThan"
  RATIO_FILTER = "RatioFilter"
  WINDOW_CUTOFF = "WindowLenFilter"

  NO_OP = "ExtendOperator"  # ExtendOperator without config == NoOp
  DUMMY_RECEIVER = "DummyReceiver"
  SEND_K = "SendK"
  RATE_RECEIVER = "RateRecordReceiver"
  ECHO = "EchoOperator"
  RAND_SOURCE = "RandSourceOperator"
  RAND_EVAL = "RandEvalOperator"
  RAND_HIST = "RandHistOperator"
  TIMEWARP = "ExperimentTimeRewrite"
  FILTER_SUBSCRIBER = "FilterSubscriber"
  AVG_CONGEST_LOGGER = "AvgCongestLogger"

  TIME_SUBSCRIBE = "TimeBasedSubscriber"
  VAR_TIME_SUBSCRIBE = "VariableCoarseningSubscriber"
  ONE_SHOT_SUBSCRIBE = "OneShotSubscriber"
  DELAYED_SUBSCRIBE = "DelayedOneShotSubscriber"

  LATENCY_MEASURE_SUBSCRIBER = "LatencyMeasureSubscriber"

  TPUT_WORKER = "MultiRoundSender"
  TPUT_CONTROLLER = "MultiRoundCoordinator"

  # Supported by Python local controller/worker only
  UNIX = "Unix"
  FETCHER = "Fetcher"

  SEQ_TO_RATIO = "SeqToRatio"
  
  BLOB_READ = "BlobReader"
  INTERVAL_SAMPLING = "IntervalSamplingOperator"
  
  
def check_ts_field(in_schema, cfg):
  if 'ts_field' in cfg:
    ts_field = int(cfg['ts_field'])
    if ts_field >= len(in_schema):
      raise SchemaError('ts_field %d illegal for operator; only %d real inputs' \
        % (ts_field, len(in_schema)))
    if in_schema[ts_field][0] != 'T':
      raise SchemaError('Expected a time element for ts_field %d' % ts_field)

def is_numeric(in_schema, cfg, field_name, op_name):
  return is_of_type(in_schema, cfg, field_name, op_name, 'ID')

def is_string(in_schema, cfg, field_name, op_name):
  return is_of_type(in_schema, cfg, field_name, op_name, 'S')

def is_int(in_schema, cfg, field_name, op_name):
  return is_of_type(in_schema, cfg, field_name, op_name, 'I')

SUMMARY_TYPES = ["Histogram", "Sketch", "Sample"]

def is_summary(in_schema, cfg, field_name, op_name):
  return is_of_type(in_schema, cfg, field_name, op_name, SUMMARY_TYPES)


def is_of_type(in_schema, cfg, field_name, op_name, valid_types):
  if field_name not in cfg:
    raise SchemaError("Must specify %s for %s" % (field_name,op_name))
  fld = int( cfg[field_name] )
  if len(in_schema) <= fld:
    raise SchemaError("Only %d fields for input to %s; %s was %i" % \
      (len(in_schema), op_name, field_name,fld))
  if in_schema[fld][0] not in valid_types:
    raise SchemaError("Field %s [%i] of %s was of type %s"\
      (field_name, fld, op_name, in_schema[fld][0]))
  return True

def validate_FileRead(in_schema, cfg):
  if len(in_schema) > 0:
    raise SchemaError("File Read should take no inputs, but got %s" % str(in_schema))
  return [("S","")]

def validate_BlobRead(in_schema, cfg):
  if len(in_schema) > 0:
    raise SchemaError("Blob Read should take no inputs, but got %s" % str(in_schema))
  return [("S","filename"), ('B', "contents")]

def validate_grep(in_schema, cfg):
  fld = cfg['id']
  if fld >= len(in_schema) or in_schema[fld][0] != 'S':
    raise SchemaError("Can't grep on field %d of %s" % (fld, str(in_schema)))
  return in_schema

def validate_parse(in_schema, cfg):
#  types':"DSS", 'field_to_parse
  field_to_parse = int(cfg['field_to_parse'])

  parsed_field_type = in_schema[field_to_parse][0]
  if parsed_field_type != 'S':
    raise SchemaError("GenericParse needs string field to parse, got: "\
                      "{0}".format(parsed_field_type))

  allowed_bool_str = [str(val).lower() for val in [True, False]]
  keep_option = cfg['keep_unparsed'] = cfg['keep_unparsed'].lower()

  if not keep_option in allowed_bool_str:
    raise SchemaError("Needed one of (case-insensitive) {0} for "
                      "'keep_unparsed' generic parse config field, got: "
                      "{1}".format(str(allowed_bool_str), keep_option))

  keep_unparsed = keep_option.lower() == 'true'

  ret = []

  if keep_unparsed:
    ret.extend( in_schema[0:field_to_parse] )

  for c in cfg['types']:
    ret.append((c, ''))
  if keep_unparsed:
    ret.extend(in_schema[field_to_parse + 1:])
  return ret

def validate_extend(in_schema, cfg):
#  print "extend with cfg",cfg
  newS = []
  newS.extend(in_schema)
  for t in cfg['types']:
    newS.append((t.upper(), ''))
  return newS

def validate_timestamp(in_schema, cfg):
  newS = []
  newS.extend(in_schema)
  if cfg["type"]=="s":
    newS.append( ("T", 'timestamp(s)') )
  elif cfg["type"]=="ms":
    newS.append( ("D", 'timestamp(ms)') )
  elif cfg["type"]=="us":
    newS.append( ("D", 'timestamp(us)') )
  else:
    raise SchemaError("Needed time granularity specifier, got: " + cfg["type"])
  return newS

def validate_latency_measure(in_schema, cfg):
  newS = [("S", 'host name'), ('S', 'metric name'), ('I', 'bucket'), ('I','count')]
  return newS

def validate_TRound(in_schema, cfg):
  fld_offset = int(cfg['fld_offset'])
  if fld_offset >= len(in_schema):
    raise SchemaError("can't round field %d since input only has %d fields (%s)." % \
         (fld_offset, len(in_schema), str(in_schema)))
    
  roundable_types = list('TDI')
  t = in_schema[fld_offset][0]
  if t not in roundable_types:
    raise SchemaError("rounding operator requires that field %d be a time, instead was %s" % (fld_offset,t))
  # NC being sneaky here...
  cfg['in_type'] = t

  out_schema = list(in_schema)
  out_schema[fld_offset] = ('T', in_schema[fld_offset][1])
  return out_schema

def validate_RandEval(in_schema, cfg):
  in_types = [ty for ty,name in in_schema[0:3]]
  if in_types != ['S','T', 'I']:
    raise SchemaError("rand eval requires inputs Str, Time, Int. Got %s" % str(in_schema))
    
  return []    

def validate_CSVParse(in_schema, cfg):
  if in_schema[0][0] != 'S':
    raise SchemaError("CSVParse currently requires a string as the first "\
                       "element of an input tuple")

  valid_types = ['S', 'D', 'I']
  if any(t not in valid_types for t in cfg['types']):
    raise SchemaError("CSVParse currently only accepts string, double, and "\
                      "32-bit integer types")

  types = cfg['types']
  if cfg['fields_to_keep'] == 'all':
    return [(t, '') for t in types]

  try:
    flds_to_keep = map(int, cfg['fields_to_keep'].split())
    return [(types[field], '') for field in flds_to_keep]
  except ValueError as e:
    raise SchemaError("Needed field indices. " + str(e))

def validate_Quantile(in_schema, cfg):
  fld = cfg["field"]
  if len(in_schema) <= fld:
    raise SchemaError("not enough fields in quantile input")
  if in_schema[fld][0] not in SUMMARY_TYPES:
    err = "Can only take quantile of a summary; instead got " + in_schema[fld][0]
    raise SchemaError(err) 

  newS = []
  newS.extend(in_schema)
  newS[fld] = ('I', cfg["q"]+'-quantile of '+in_schema[fld][1])
  return newS  

def validate_S2Count(in_schema, cfg):
  fld = cfg["field"]
  if len(in_schema) <= fld:
    raise SchemaError("not enough fields in quantile input")
  if in_schema[fld][0] not in SUMMARY_TYPES:
    err = "Can only take quantile of a summary; instead got " + in_schema[fld][0]
    raise SchemaError(err) 

  newS = []
  newS.extend(in_schema)
  newS.append( ('I', 'count(%s)' % in_schema[fld][1]) )
  return newS  
  
  
def validate_ToSummary(in_schema, cfg):
  fld = cfg["field"]
  if len(in_schema) <= fld:
    err = "not enough fields in ToSummary input; needed %d got %d" % (fld+1, len(in_schema))
    raise SchemaError(err)
  
  if in_schema[fld][0] != 'I':
    raise SchemaError("Can only put ints into summaries, for now; got %s" % str(in_schema[fld]))
  
  newS = []
  newS.extend(in_schema)
  newS[fld] = ('Histogram', 'summary of '+in_schema[fld][1])
  return newS  
  
  
def validate_Projection(in_schema, cfg):
  field = int(cfg['field'])
  new_schema = in_schema[0:field]
  new_schema.extend(in_schema[field+1:])
  return new_schema

def validate_Timewarp(in_schema, cfg):
  fld_offset = int(cfg['field'])
  if fld_offset >= len(in_schema):
    raise SchemaError("can't round field %d since input only has %d fields (%s)." % \
         (fld_offset, len(in_schema), str(in_schema)))
    
  roundable_types = list('TD')
  t = in_schema[fld_offset][0]
  if t not in roundable_types:
    raise SchemaError("rounding operator requires that field %d be a time, instead was %s" % (fld_offset,t))
  out_schema = list(in_schema)
  out_schema[fld_offset] = ('T', in_schema[fld_offset][1])
  return out_schema

def validate_URLToDomain(in_schema, cfg):
  is_string(in_schema, cfg, "field", OpType.URLToDomain) 
  return in_schema    
  
def validate_CountLogger(in_schema, cfg):
  fld = cfg['field']
  if fld >= len(in_schema) or in_schema[fld][0] != 'I':
    raise SchemaError("Can't tabulate field %d of %s" % (fld, str(in_schema)))
  return in_schema

def validate_FilterSubscriber(in_schema, cfg):
  return in_schema

def validate_Tput_Control(in_schema, cfg):
  check_ts_field(in_schema, cfg)
  if "num_results" not in cfg:
    raise SchemaError("must specify num_results for tput")
  if "sort_column" not in cfg:
    raise SchemaError("must specify sort_column for tput")
  return in_schema


def validate_SeqToRatio(schema,cfg):
  is_numeric(schema, cfg, "total_field", OpType.SEQ_TO_RATIO) 
  is_int(schema, cfg, "respcode_field", OpType.SEQ_TO_RATIO) 
  is_string(schema, cfg, "url_field", OpType.SEQ_TO_RATIO) 
  s2 = []
  s2.extend(schema)
  s2.append(  ('D', 'ratio')  ) 
  return s2

# Schemas are represented as a function that maps from an input schema and configuration
# to an output schema
# A schema itself is a list of pairs, where the first element is a typecode [I,D,S, or T]
# and the second field is a name for that element.
SCHEMAS = {} 
SCHEMAS[OpType.FILE_READ] = validate_FileRead
SCHEMAS[OpType.BLOB_READ] = validate_BlobRead
SCHEMAS[OpType.STRING_GREP] = validate_grep
SCHEMAS[OpType.PARSE] = validate_parse
SCHEMAS[OpType.EXTEND] = validate_extend
SCHEMAS[OpType.TIMESTAMP] = validate_timestamp
SCHEMAS[OpType.LATENCY_MEASURE_SUBSCRIBER] = validate_latency_measure
SCHEMAS[OpType.T_ROUND_OPERATOR] = validate_TRound
SCHEMAS[OpType.EQUALS_FILTER] = lambda schema,cfg: schema if \
  is_numeric(schema, cfg, "field", OpType.EQUALS_FILTER) else None
SCHEMAS[OpType.RATIO_FILTER] = lambda schema,cfg: schema if \
  is_numeric(schema, cfg, "numer_field", OpType.RATIO_FILTER) and \
    is_numeric(schema, cfg, "denom_field", OpType.RATIO_FILTER) else None


SCHEMAS[OpType.VARIABLE_SAMPLING] = lambda schema,cfg: schema 
SCHEMAS[OpType.INTERVAL_SAMPLING] = lambda schema,cfg: schema 

SCHEMAS[OpType.CONGEST_CONTROL] = lambda schema,cfg: schema 


SCHEMAS[OpType.ECHO] = lambda schema,cfg: schema 
SCHEMAS[OpType.SEND_K] =  lambda schema,cfg: [('I','K')]
SCHEMAS[OpType.RATE_RECEIVER] = lambda schema,cfg: schema
SCHEMAS[OpType.RAND_SOURCE] = lambda schema,cfg: [('S','state'), ('T', 'timestamp')]
SCHEMAS[OpType.RAND_HIST] = lambda schema,cfg: [('T', 'timestamp'), ('I', 'dummy key'), \
   ('Histogram', 'dummy data')]
SCHEMAS[OpType.RAND_EVAL] = validate_RandEval
# TODO RAND_EVAL
#  SCHEMAS[NO_OP] = lambda x: x
SCHEMAS[OpType.PROJECT] = validate_Projection
SCHEMAS[OpType.DUMMY_RECEIVER] = lambda schema,cfg: None

SCHEMAS[OpType.UNIX] =  lambda schema,cfg: [("S","")]
SCHEMAS[OpType.URLToDomain] = validate_URLToDomain


SCHEMAS[OpType.CSV_PARSE] = validate_CSVParse
SCHEMAS[OpType.QUANTILE] = validate_Quantile
SCHEMAS[OpType.DEGRADE_SUMMARY] = lambda schema,cfg:  schema if \
  is_summary(schema, cfg, "field", OpType.DEGRADE_SUMMARY) else None
  
SCHEMAS[OpType.TO_SUMMARY] = validate_ToSummary
SCHEMAS[OpType.SUMMARY_TO_COUNT] = validate_S2Count
SCHEMAS[OpType.TIMEWARP] = validate_Timewarp
SCHEMAS[OpType.COUNT_LOGGER] = validate_CountLogger
SCHEMAS[OpType.AVG_CONGEST_LOGGER] = lambda schema,cfg: schema
SCHEMAS[OpType.TPUT_CONTROLLER] = validate_Tput_Control
SCHEMAS[OpType.TPUT_WORKER] = lambda schema,cfg: schema

SCHEMAS[OpType.SEQ_TO_RATIO] = validate_SeqToRatio
SCHEMAS[OpType.WINDOW_CUTOFF] = lambda schema,cfg: schema


#SCHEMAS[OpType.FILTER_SUBSCRIBER] = validate_FilterSubscriber
# is a special case
