#include "base_operators.h"

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/range/istream_range.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/numeric/conversion/cast.hpp>

#include <boost/asio/ip/host_name.hpp>
#include <boost/interprocess/detail/atomic.hpp>


#include "js_utils.h"

#if __cplusplus <= 199711L
#include "strtk.hpp"
#endif

//using namespace std;
using std::string;
using std::vector;
using std::map;
using std::istringstream;
using boost::shared_ptr;

namespace jetstream {



const int LINES_PER_EMIT = 4000; //can cut down at least to 1000 without performance penalty
      //if useful.
const unsigned MS_PER_READ = 0; //should raise this if we're going to use as a tailer.

int
CFileRead::emit_data() {

  if (!in_file.is_open()) {
    in_file.open (f_name.c_str());
    if (in_file.fail()) {
      LOG(WARNING) << "could not open file " << f_name.c_str();
      return -1; //stop
    }
  }
  
  vector<shared_ptr<Tuple> > tuples;
  tuples.reserve(LINES_PER_EMIT);
  DataplaneMessage no_meta;
//  LOG(INFO) << "starting loop, " << tuples.size() << " tuples";
  
  for (int i = 0; i < LINES_PER_EMIT; ++i) {
    // ios::good checks for failures in addition to eof
    if (!in_file.good()) {
      break;
    }
    string line;

    getline(in_file, line);
    if (skip_empty && line.length() == 0) {
      continue;
    }
    shared_ptr<Tuple> t( new Tuple);
    Element * e = t->add_e();
    e->set_s_val(line);
    t->set_version(lineno++);
    tuples.push_back(t);
  }


//  LOG(INFO) << "Calling chain::process, " << tuples.size() << " tuples";
  chain->process(tuples, no_meta);
//  LOG(INFO) << "Returned from chain::process";
  
  return in_file.good() ? MS_PER_READ : -1;
}


std::string
CFileRead::long_description() const {
  std::ostringstream buf;
  buf << "reading " << f_name << ", at line " << lineno;
  return buf.str();
}


operator_err_t
CFileRead::configure(map<string,string> &config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    LOG(WARNING) << "no file to read, bailing";
    return operator_err_t("option 'file' not specified");
  }

  boost::algorithm::to_lower(config["skip_empty"]);
  // TODO which values of config["skip_empty"] convert to which boolean
  // values?
  std::istringstream(config["skip_empty"]) >> std::boolalpha >> skip_empty;

  return NO_ERR;
}

void
ExtendOperator::process_one(boost::shared_ptr<Tuple>& t) {
  for (u_int i = 0; i < new_data.size(); ++i) {
    Element * e = t->add_e();
    e->CopyFrom(new_data[i]);
  }
}

/*
void
ExtendOperator::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {
  mutate_tuple(oldV);
  mutate_tuple(*newV);
  emit(oldV, newV);
} */


operator_err_t
ExtendOperator::configure (std::map<std::string,std::string> &config) {

  string field_types = boost::to_upper_copy(config["types"]);
  static boost::regex re("[SDI]+");

  if (!regex_match(field_types, re)) {
    LOG(WARNING) << "Invalid types for regex fields; got " << field_types;
    return operator_err_t("Invalid types for regex fields; got " + field_types);
    //should return failure here?
  }

  string first_key = "0";
  string last_key = ":";
  map<string, string>::iterator it = config.find(first_key);
  map<string, string>::iterator end = config.upper_bound(last_key);

  u_int i;
  for (i = 0;  i < field_types.size() && it != end; ++i, ++it) {
    string s = it->second;
    Element e;
    if (s == "${HOSTNAME}") {
      assert(field_types[i] == 'S');
      e.set_s_val( boost::asio::ip::host_name());
    }
    else {
      parse_with_types(&e, s, field_types[i]);
    }
    new_data.push_back(e);
  }
  if (i < field_types.size()) {
    LOG(WARNING) << "too many type specifiers for operator";
    return operator_err_t("too many type specifiers for operator");
  }
  if ( it != end ) {
    LOG(WARNING) << "not enough type specifiers for operator";
    return operator_err_t("not enough type specifiers for operator");
  }
  return NO_ERR;
}


operator_err_t
CSVParse::configure(map<string,string> &config) {
  //istringstream(config["id"]) >> fieldID;
  types = config["types"];
  string keep = config["fields_to_keep"];

  n_fields = types.length();

  discard_off_size = ((config["discard_off_size"].size() == 0) || config["discard_off_size"] != "false");
  LOG(INFO) << "CSV Parse. discard_off_size is " << discard_off_size;

  if (string("all") == keep) {
    for (int i = 0; i < n_fields; i++)
      keep_fields.push_back(true);
  } else {
    // skip all fields unless told to keep them
    for (int i = 0; i < n_fields; i++)
      keep_fields.push_back(false);

    // mark "true" at each specified position
    std::istringstream sscanf(keep);
    BOOST_FOREACH( int fld_to_keep, boost::istream_range<int>(sscanf)) {
      if (!sscanf)
        return operator_err_t("Invalid \"fields to keep\" string.");

      if (fld_to_keep >= n_fields) {
        string err_fmt("%d types given; %d is too high for field index.");
        return operator_err_t((boost::format(err_fmt) % n_fields % fld_to_keep).str());
      }

      keep_fields[fld_to_keep] = true;
    }
  }
  
  return NO_ERR;
}

void
CSVParse::process_one(boost::shared_ptr<Tuple>& t) {
  // FIXME assume we want to parse 0th element
  const Element& e = t->e(0);

  if (!e.has_s_val()) {
    LOG(WARNING) << "received tuple but element" << 0 << " is not string, ignoring";
    t.reset();
    return;
  }
  try {
    boost::tokenizer<boost::escaped_list_separator<char> > csv_parser(e.s_val());

    shared_ptr<Tuple> t2(new Tuple);
    t2->set_version(t->version());

    int i = 0;
    BOOST_FOREACH(string csv_field, csv_parser) {
      if (i >= n_fields) {
        LOG_IF(FATAL, !discard_off_size) << "Parsed more fields than types specified. Entry was "
          << e.s_val();
      } else {
        if (keep_fields[i])
          parse_with_types(t2->add_e(), csv_field, types[i]);
      }
      i++;
    }
    if (!discard_off_size || (i == n_fields))
      t = t2;
    else
      t.reset();
    
  } catch (boost::escaped_list_error err) {
    LOG_FIRST_N(WARNING, 20) << err.what() << " on " << e.s_val();
  }
  // assume we don't need to pass through any other elements...
  // TODO unassume
}

std::string
CSVParse::long_description() const {
  return "";
}

#if __cplusplus <= 199711L
void
CSVParseStrTk::process_one(boost::shared_ptr<Tuple>& t) {
  // FIXME assume we want to parse 0th element
  const Element& e = t->e(0);

  if (!e.has_s_val()) {
    LOG(WARNING) << "received tuple but element" << 0 << " is not string, ignoring";
    return;
  }
  try {
   // boost::tokenizer<boost::escaped_list_separator<char> > csv_parser(e.s_val());

   strtk::token_grid::options options;
   options.column_delimiters = " ,";
   options.support_dquotes = true;

   string val = e.s_val();
   val += "\n";
   strtk::token_grid grid(val, val.size(), options);

    shared_ptr<Tuple> t2(new Tuple);
    t2->set_version(t->version());

    strtk::token_grid::row_type row = grid.row(0);
    int i=0;
    for (std::size_t c = 0; c < row.size(); ++c) 
    {
      if (i >= n_fields) {
        //LOG(INFO) << "Parse I=" << i << " Res = |" << row.get<std::string>(c) <<"|";
        LOG_IF(FATAL, !discard_off_size) << "Parsed more fields than types specified. Entry was "
          << e.s_val();
      } else {
        //LOG(INFO) << "Parse I=" << i << " Res = |" << row.get<std::string>(c)<<"|";
        if (keep_fields[i])
          parse_with_types(t2->add_e(), row.get<std::string>(c), types[i]);
      }
      i++;
    }
    if (row.get<std::string>(row.size()-1).size() == 0)
      i--;
    if (!discard_off_size || (i == n_fields)) {
      t = t2;
    }
    else {
      t.reset();
      //LOG(INFO)  << "Not Emitting "<< discard_off_size << " i " << i << " nf " << n_fields;
    }
  } catch (boost::escaped_list_error err) {
    LOG_FIRST_N(WARNING, 20) << err.what() << " on " << e.s_val();
  }
  // assume we don't need to pass through any other elements...
  // TODO unassume
}
#endif

operator_err_t
StringGrep::configure(map<string,string> &config) {
  string pattern = config["pattern"];
  std::istringstream(config["id"]) >> fieldID;
  if (pattern.length() == 0) {
    LOG(WARNING) << "no regexp pattern specified, bailing";
    return operator_err_t("No regex specified (option 'pattern')");
  } else {
    LOG(INFO) << "starting grep operator " << id() << " with pattern " << pattern;
  }
  re.assign(pattern);
  return NO_ERR;
}


bool
StringGrep::should_emit (const Tuple& t) {
  if (re.empty()) {
    LOG(WARNING) << "no pattern assigned; did you start the operators properly?";
    return false;
  }

  if (t.e_size() == 0) {
    LOG(INFO) << "received empty tuple, ignoring";
    return false;
  }

  const Element& e = t.e(fieldID);
  if (!e.has_s_val()) {
    LOG(WARNING) << "received tuple but element" << fieldID << " is not string, ignoring";
    return false;
  }

  boost::smatch matchResults;
  bool found = boost::regex_search(e.s_val(), matchResults, re);
  return found;
}


std::string
StringGrep::long_description() const {
  std::ostringstream buf;
  buf << "filtering for "<< re.str() << " in field " << fieldID;
  return buf.str();
}


operator_err_t
GenericParse::configure(std::map<std::string,std::string> &config) {
  string pattern = config["pattern"];

  // TODO is this "to_lower" call necessary?
  boost::algorithm::to_lower(config["keep_unparsed"]);
  istringstream(config["keep_unparsed"]) >> std::boolalpha >> keep_unparsed;

  try {
    re.assign(pattern);
  } catch( boost::regex_error e) {
    return operator_err_t("regex " + pattern + " did not compile:" +
        e.std::exception::what());
  }

  istringstream(config["field_to_parse"]) >> fld_to_parse;
  if (fld_to_parse < 0 || fld_to_parse > 100) {
    LOG(WARNING) << "field ID " << fld_to_parse << "looks bogus";
  }

  field_types = boost::to_upper_copy(config["types"]);
  static boost::regex fld_types("[SDI]+");

  if (!regex_match(field_types, fld_types)) {
    LOG(WARNING) << "Invalid types for regex fields; got " << field_types;
    return operator_err_t("Invalid types for regex fields; got " + field_types);
  }

  if (pattern.length() == 0) {
    LOG(WARNING) << "no regexp pattern specified, bailing";
    return operator_err_t("no regexp pattern specified");
  }
  //TODO could check re.max_size() against field_types.length()
  return NO_ERR;
}

void parse_with_types(Element * e, const string& s, char typecode) {
  switch (typecode) {
    case 'I':
      {
        int i;
        istringstream parser(s);
        parser >> i;
        if (!parser)
          LOG(WARNING) << "parse_with_type failed for type " << typecode << " and string " << s;
        e->set_i_val( i );
        break;
      }
    case 'D':
      {
        double d;
        istringstream parser(s);
        parser >> d;
        if (!parser)
          LOG(WARNING) << "parse_with_type failed for type " << typecode << " and string " << s;
        e->set_d_val( d );
        break;
      }
    case 'S':
      {
        e->set_s_val( s );
        break;
      }
    default:
      LOG(FATAL) << "should be impossible to have typecode " << typecode;
  }
}

void
GenericParse::process_one(boost::shared_ptr<Tuple>& t) {
  shared_ptr<Tuple> t2(new Tuple);

  if (keep_unparsed) {
    for(int i = 0; i < t->e_size() && i < fld_to_parse; ++i) {
      Element * e = t2->add_e();
      e->CopyFrom(t->e(i));
    }
    t2->set_version(t->version());
  }

  if (fld_to_parse >= t->e_size()) {
    LOG(WARNING) << "can't parse field " << fld_to_parse << "; total size is only" << t->e_size();
  }

  boost::smatch matchResults;

  bool found = false;

  try {
      found = boost::regex_match(t->e(fld_to_parse).s_val(), matchResults, re);
  }
  catch (std::exception &e) {
      LOG(FATAL) << "Regex match error thrown: " << e.what();
  }

  if (found) {
    if (matchResults.size() != field_types.length() + 1) {
      LOG(FATAL) << "regex for " << id() << " has " << matchResults.size() <<
          "fields but we only have " << field_types.length() << "types.";
    }

    for (size_t fld = 1; fld < matchResults.size(); ++ fld) {
      string s = matchResults.str(fld);
      char typecode = field_types[fld-1];
      Element * e = t2->add_e();
      parse_with_types(e, s, typecode);
    }
  }
  else {
    LOG(WARNING) << "No parse matches on string: " << t->e(fld_to_parse).s_val();
    // what do we do on parse failures?  Currently, suppress silently as 'no match'
    return;
  }

  if (keep_unparsed)
    for (int i = fld_to_parse+1; i < t->e_size(); ++i) {
      Element * e = t2->add_e();
      e->CopyFrom(t->e(i));
    }

  t = t2;
}

bool
SampleOperator::should_emit (const Tuple& t) {
  uint32_t v = gen();
  return (v >= boost::interprocess::ipcdetail::atomic_read32(&threshold));
}

operator_err_t
SampleOperator::configure (std::map<std::string,std::string> &config) {
  double frac_to_drop = 0;
  istringstream(config["fraction"]) >> frac_to_drop;
  threshold = frac_to_drop * std::numeric_limits<uint32_t>::max();
  int seed = 0;
  istringstream(config["seed"]) >> seed;
  gen.seed(seed);
  return NO_ERR;
}



bool
HashSampleOperator::should_emit (const Tuple& t) {

  uint32_t hashval = 0;
  const Element& e = t.e(hash_field);
  switch(hash_type) {
    case 'I': {
      int val = e.i_val();
      hashval = jenkins_one_at_a_time_hash((char *) &val, sizeof(val));
      //LOG_FIRST_N(INFO, 20)<< "Sanity check val "<< val << " hash " << hashval << " threshold " << boost::interprocess::ipcdetail::atomic_read32(&threshold) << " tuple " << fmt(*t);
      break;
    }
    case 'D': {
      double val = e.d_val();
      hashval = jenkins_one_at_a_time_hash((char *) &val, sizeof(val));
      break;
    }

    case 'S': {
      const string& val = e.s_val();
      hashval = jenkins_one_at_a_time_hash(val.c_str(), val.length());
      break;
    }

    case 'T': {
      time_t val = e.t_val();
      hashval = jenkins_one_at_a_time_hash((char *) &val, sizeof(val));
      break;
    }

    default:
      LOG(FATAL) << "must specify hash field type; was " << hash_type;
  }
  return (hashval >= boost::interprocess::ipcdetail::atomic_read32(&threshold));

}


operator_err_t
HashSampleOperator::configure (std::map<std::string,std::string> &config) {
  double frac_to_drop = 0;
  istringstream(config["fraction"]) >> frac_to_drop;
  threshold = frac_to_drop * std::numeric_limits<uint32_t>::max();

  if( !(std::stringstream(config["hash_field"]) >> hash_field)) {
    return operator_err_t("hash_field must be an int");
  }

//  if((config["debug_stage"].length() > 0) && !(stringstream(config["debug_stage"]) >> debug_stage)) {
//    return operator_err_t("debug_stage must be an int");
//  }

  if(config["hash_type"].length() < 1) {
    return operator_err_t("hash_type must be defined");
  } else
    hash_type = config["hash_type"][0];
    LOG(INFO) << "Configured hashSample field " << hash_field <<" ("<< config["hash_field"] <<") type "<< hash_type;
    return NO_ERR;
}


void
TRoundingOperator::process_one (boost::shared_ptr<Tuple>& t) {
  if (in_type == T) {
    time_t old_val = t->e(fld_offset).t_val();
    t->mutable_e(fld_offset)->set_t_val((old_val / round_to) * round_to + add_offset);
  }

  if (in_type == I) {
    int old_val = t->e(fld_offset).i_val();
    t->mutable_e(fld_offset)->clear_i_val();
    t->mutable_e(fld_offset)->set_t_val(boost::numeric_cast<time_t>((old_val / round_to) * round_to) + add_offset);
  }

  if (in_type == D) {
    double old_val = t->e(fld_offset).d_val();
    t->mutable_e(fld_offset)->clear_d_val();
    t->mutable_e(fld_offset)->set_t_val(boost::numeric_cast<time_t>(old_val / round_to) * round_to + add_offset);
  }
}

operator_err_t
TRoundingOperator::configure (std::map<std::string,std::string> &config) {

  if ( !(istringstream(config["round_to"]) >> round_to)) {
    return operator_err_t("must specify an int as round_to");
  }

  if ( !(istringstream(config["fld_offset"]) >> fld_offset)) {
    return operator_err_t("must specify an int as fld_offset; got " + config["fld_offset"] +  " instead");
  }

  if (!(istringstream(config["add_offset"]) >> add_offset)) {
    return operator_err_t("must specify number to add to result; got " + config["add_offset"] + "instead");
  }

  if (config["in_type"].length() != 1)
    return operator_err_t("Invalid input type: " + config["in_type"]);

  switch (config["in_type"][0]) {
    case 'T':
      in_type = T;
      break;
    case 'D':
      in_type = D;
      break;
    case 'I':
      in_type = I;
      break;
    default:
      return operator_err_t("Invalid input type: " + config["in_type"]);
  }

  return NO_ERR;
}
/*

operator_err_t
UnixOperator::configure (std::map<std::string,std::string> &config) {
  cmd = config["cmd"];
  if (cmd.length() == 0) {
    LOG(WARNING) << "no cmd to run, bailing";
    return operator_err_t("option 'cmd' not specified");
  }
  pipe = popen(cmd.c_str(), "r+");

  if(NULL == pipe) {
    LOG(WARNING) << "popen failed return = " << pipe <<"; errno =" <<errno <<"; strerror=" << strerror(errno) << "; cmd= "<<cmd;
    return operator_err_t("popen failed");
  }

  return NO_ERR;
}


void
UnixOperator::stop() {
  pclose(pipe);
  ThreadedSource::stop();
}

void UnixOperator::process (boost::shared_ptr<Tuple> t) {
  string s = fmt(*t) + "\n";
  const char * buf = s.c_str();
  write(fileno(pipe), buf, s.length());
//  fputs(s.c_str(), pipe);
  fsync(fileno(pipe));
  cout << "returning from 'process'" << endl;
}


bool
UnixOperator::emit_1() {
  char buf[1000];
  buf[0] = 0;
  cout << "reading line from unix cmd..." << endl;

//  fgets(buf, sizeof(buf), pipe);
//  int readLen = strlen(buf);

  int readLen = read( fileno(pipe), buf, sizeof(buf) - 1);
  buf[readLen] = 0;
  cout << "read: " << buf << endl;
  if( readLen > 0) {
    vector <string> lines;

    boost::split( lines, buf, is_any_of( "\n" ) );

    for (unsigned int i=0; i < lines.size(); ++i) {
      if (lines[i].length() == 0)
        continue;
      shared_ptr<Tuple> t( new Tuple);
      t->set_version(line_count++);
      Element * e = t->add_e();
      e->set_s_val(lines[i]);
      emit(t);
    }
  }
  return true; //we're done if we failed to read.
}*/


operator_err_t
TimestampOperator::configure (std::map<std::string,std::string> &config) {
  if("s" == config["type"])
    type = S;
  else if("ms" == config["type"])
    type = MS;
  else if("us" == config["type"])
    type = US;
  else
    return operator_err_t("type is not s, ms, us");
  return NO_ERR;
}

void
TimestampOperator::process_one (boost::shared_ptr<Tuple>& t) {
  Element * e = t->add_e();
  if(type == S) {
    sec_t time = get_sec();
    e->set_i_val(time);
  }
  if(type == MS) {
    usec_t time = get_usec();
    e->set_d_val((double)(time/1000));
  }
  if(type == US) {
    usec_t time = get_usec();
    e->set_d_val((double)time);
  }
}


void
URLToDomain::process_one (boost::shared_ptr<Tuple>& t) {
  string url = t->e(field_id).s_val();
  
  vector <string> chunks;

  boost::split( chunks, url, boost::is_any_of("/"));
  if (chunks.size() >= 3) {
    t->mutable_e(field_id)->set_s_val(chunks[2]);
  } //else pass through unchanged
}

operator_err_t
URLToDomain::configure (std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field_id)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }
  return NO_ERR;
}


void
ProjectionOperator::process_one (boost::shared_ptr<Tuple>& t) {
  //assert(t->e_size() >= 1);
  uint new_size = t->e_size() - 1;
  google::protobuf::RepeatedPtrField<Element>* elems = t->mutable_e();
  for (unsigned i = field_id; i < new_size; ++i) {
    elems->SwapElements(i, i+1);
  }
  elems->RemoveLast();
}

operator_err_t
ProjectionOperator::configure (std::map<std::string,std::string> &config) {
  if (!config.count("field") || !(istringstream(config["field"]) >> field_id)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }
  return NO_ERR;
}



bool
GreaterThan::should_emit (const Tuple& t) {
  double val = jetstream::numeric(t, field_id);
  return (val > bound);
}

operator_err_t
GreaterThan::configure (std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field_id)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }

  if ( !(istringstream(config["bound"]) >> bound)) {
    return operator_err_t("must specify bound; got " + config["bound"] +  " instead");
  }
  return NO_ERR;
}

bool
IEqualityFilter::should_emit (const Tuple& t) {
  int val = t.e(field_id).i_val();
  return (val == targ);
}

operator_err_t
IEqualityFilter::configure (std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["field"]) >> field_id)) {
    return operator_err_t("must specify an int as field; got " + config["field"] +  " instead");
  }

  if ( !(istringstream(config["targ"]) >> targ)) {
    return operator_err_t("must specify targ; got " + config["targ"] +  " instead");
  }
  return NO_ERR;
}



bool
RatioFilter::should_emit (const Tuple& t) {
  double denom = jetstream::numeric(t, denom_field_id);
  double numer = jetstream::numeric(t, numer_field_id);
  
//  cout << "ratio was " << (numer/denom) << endl;
  return ( denom == 0 ||  numer / denom > bound);
}

operator_err_t
RatioFilter::configure (std::map<std::string,std::string> &config) {
  if ( !(istringstream(config["denom_field"]) >> denom_field_id)) {
    return operator_err_t("must specify an int as field; got " + config["denom_field"] +  " instead");
  }

  if ( !(istringstream(config["numer_field"]) >> numer_field_id)) {
    return operator_err_t("must specify an int as field; got " + config["numer_field"] +  " instead");
  }

  if ( !(istringstream(config["bound"]) >> bound)) {
    return operator_err_t("must specify bound; got " + config["bound"] +  " instead");
  }
  return NO_ERR;
}

operator_err_t
WindowLenFilter::configure (std::map<std::string,std::string> &config) {
  bound = UINT_MAX;
  err_bound_lev = 0;
  err_field = -1;
  if ( config["err_field"].length() > 0 &&  !(istringstream(config["err_field"]) >> err_field)) {
    return operator_err_t("must specify err_field as int; got " + config["err_field"] +  " instead");
  }
  LOG(INFO) << "assuming sort was based on field " << err_field;
  
  return NO_ERR;
}


static const int LEVELS = 20;

void
WindowLenFilter::process (OperatorChain * chain,
                          std::vector<boost::shared_ptr<Tuple> > & tuples,
                          DataplaneMessage& window_marker) {
  boost::lock_guard<boost::mutex> lock (boost::mutex);

  if ( k_in_win + tuples.size() > bound ) { //won't pass all tuples
    unsigned tuples_to_pass = bound - k_in_win;
    
    if ( (err_field > -1) && (err_bound_lev == 0) ) {
      err_bound_lev = jetstream::numeric(*tuples[tuples_to_pass], err_field);
      LOG(INFO) << "------- Local cutoff: "<<err_bound_lev << "-------";
    }
    
    tuples.resize(tuples_to_pass);  
  }
  k_in_win += tuples.size();
  

  
  if ( window_marker.type() == DataplaneMessage::END_OF_WINDOW) {
    boost::lock_guard<boost::mutex> lock (boost::mutex);
    vector<double> ratios;
    vector<unsigned> bounds;
    unsigned cur_level = LEVELS-1;
//    double fract_of_max = double(k_in_win) / bound;
//    if (fract_of_theoretical < )
    
    for( int i = 0; i < LEVELS; ++i) {
      unsigned bnd = k_in_win * double(i+1) / LEVELS;
      bounds.push_back( bnd );
      ratios.push_back( double(i+1) / LEVELS );
      if ( (cur_level == LEVELS-1) && bnd >= bound)
        cur_level = i;
    }
    bounds[LEVELS-1] = UINT_MAX;
    
    unsigned delta = congest_policy->get_step(id(), ratios.data(), ratios.size(), cur_level);
    
    bound = bounds[cur_level + delta];

    LOG_IF(INFO,delta != 0) << "Changing local thresh. New thresh is " << bound
     << " and last-window had " << k_in_win;
    LOG(INFO) << "thresholding error is " << err_bound_lev;
    window_marker.set_tput_r2_threshold(err_bound_lev);
    k_in_win = 0;
    err_bound_lev = 0;
  }
  
}




const string CFileRead::my_type_name("CFileRead operator");
const string CSVParse::my_type_name("CSVParse operator");
const string CSVParseStrTk::my_type_name("CSVParseStrTk operator");
const string StringGrep::my_type_name("StringGrep operator");
const string GenericParse::my_type_name("Parser operator");
const string ExtendOperator::my_type_name("Extend operator");
const string ProjectionOperator::my_type_name("Projection");

const string TimestampOperator::my_type_name("Timestamp operator");
//const string OrderingOperator::my_type_name("Ordering operator");

const string SampleOperator::my_type_name("Sample operator");
const string HashSampleOperator::my_type_name("Hash-sample operator");
const string TRoundingOperator::my_type_name("Time rounding");
//const string UnixOperator::my_type_name("Unix command");
const string URLToDomain::my_type_name("URL to Domain");
const string GreaterThan::my_type_name("Numeric Filter");
const string IEqualityFilter::my_type_name("Numeric Equality");
const string RatioFilter::my_type_name("Ratio Filter");
const string WindowLenFilter::my_type_name("Window cutoff");


}
