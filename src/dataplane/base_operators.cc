#include "dataplaneoperator.h"
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

using namespace std;
using namespace boost;

namespace jetstream {


operator_err_t
FileRead::configure(map<string,string> &config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    LOG(WARNING) << "no file to read, bailing" << endl;
    return operator_err_t("option 'file' not specified");
  }

  boost::algorithm::to_lower(config["skip_empty"]);
  // TODO which values of config["skip_empty"] convert to which boolean
  // values?
  istringstream(config["skip_empty"]) >> std::boolalpha >> skip_empty;

  return NO_ERR;
}

void
FileRead::start() {
  running = true;
  // Pass a reference to this object, otherwise boost makes its own copy (with its 
  // own member variables). Must ensure (*this) doesn't die before the thread exits!
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}


void
FileRead::stop() {
  running = false;
  LOG(INFO) << "stopping file read operator";
  
  assert (loopThread->get_id()!=boost::this_thread::get_id());
  loopThread->join();
}


void
FileRead::process(boost::shared_ptr<Tuple> t) {
  LOG(WARNING) << "Should not send data to a FileRead";
}


bool
FileRead::isRunning() {
  return running;
}


void
FileRead::operator()() {
  ifstream in_file (f_name.c_str());
  if (in_file.fail()) {
    LOG(WARNING) << "could not open file " << f_name.c_str() << endl;
    running = false;
    return;
  }
  string line;
  // ios::good checks for failures in addition to eof
  int lineno = 0;
  while (running && in_file.good()) {
    getline(in_file, line);
    if (skip_empty && line.length() == 0) {
        continue;
    }
    lineno ++ ;
    shared_ptr<Tuple> t( new Tuple);
    Element * e = t->add_e();
    e->set_s_val(line);
    t->set_version(lineno);
    emit(t);
  }
  running = false;
  no_more_tuples();
}

std::string
FileRead::long_description() {
  std::ostringstream buf;
  buf << "reading" << f_name;
  return buf.str();
}

operator_err_t
CSVParse::configure(map<string,string> &config) {
  //istringstream(config["id"]) >> fieldID;
  types = config["types"];
  string keep = config["fields_to_keep"];

  n_fields = types.length();

  if (string("all") == keep) {
    for (int i = 0; i < n_fields; i++)
      keep_fields.push_back(true);
    return NO_ERR;
  }

  // skip all fields unless told to keep them
  for (int i = 0; i < n_fields; i++)
    keep_fields.push_back(false);

  // mark "true" at each specified position
  istringstream sscanf(keep);
  BOOST_FOREACH( int fld_to_keep, istream_range<int>(sscanf)) {
    if (!sscanf)
      return operator_err_t("Invalid \"fields to keep\" string.");

    if (fld_to_keep >= n_fields) {
      string err_fmt("%d types given; %d is too high for field index.");
      return operator_err_t((format(err_fmt) % n_fields % fld_to_keep).str());
    }

    keep_fields[fld_to_keep] = true;
  }
  
  discard_off_size = ((config["discard_off_size"].size() > 0) && config["discard_off_size"] != "false");

  return NO_ERR;
}

void
CSVParse::process(boost::shared_ptr<Tuple> t) {
  // FIXME assume we want to parse 0th element
  const Element* e = &t->e(0);

  if (!e->has_s_val()) {
    LOG(WARNING) << "received tuple but element" << 0 << " is not string, ignoring" << endl;
    return;
  }

  boost::tokenizer<boost::escaped_list_separator<char> > csv_parser(e->s_val());

  shared_ptr<Tuple> t2(new Tuple);
  t2->set_version(t->version());

  int i = 0;
  BOOST_FOREACH(string csv_field, csv_parser) {
    if (i >= n_fields) {
      if (!discard_off_size)
        LOG(FATAL) << "Parsed more fields than types specified." << endl;
    } else {
      if (keep_fields[i])
        parse_with_types(t2->add_e(), csv_field, types[i]);
    }
    i++;
  }
  if (!discard_off_size || (i == n_fields))
    emit(t2);
  // assume we don't need to pass through any other elements...
  // TODO unassume
}

std::string
CSVParse::long_description() {
  return "CSV parser";
}

operator_err_t
StringGrep::configure(map<string,string> &config) {
  string pattern = config["pattern"];
  istringstream(config["id"]) >> fieldID;
  if (pattern.length() == 0) {
    LOG(WARNING) << "no regexp pattern specified, bailing" << endl;
    return operator_err_t("No regex specified (option 'pattern')");
  } else {
    LOG(INFO) << "starting grep operator " << id() << " with pattern " << pattern;
  }
  re.assign(pattern);
  return NO_ERR;
}


void
StringGrep::process (boost::shared_ptr<Tuple> t)
{
  assert(t);
  if (re.empty()) {
    LOG(WARNING) << "no pattern assigned; did you start the operators properly?";
    return;
  }

  if (t->e_size() == 0) {
    LOG(INFO) << "received empty tuple, ignoring" << endl;
    return;
  }

  Element* e = t->mutable_e(fieldID);
  if (!e->has_s_val()) {
    LOG(WARNING) << "received tuple but element" << fieldID << " is not string, ignoring" << endl;
    return;
  }

  boost::smatch matchResults;
  bool found = boost::regex_search(e->s_val(), matchResults, re);
  if (found) {
    // The string element matches the pattern, so push it through
    emit(t);
  }
}


std::string
StringGrep::long_description() {
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
  } catch( regex_error e) {
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
    LOG(WARNING) << "no regexp pattern specified, bailing" << endl;
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
          LOG(WARNING) << "parse_with_type failed for type " << typecode << " and string " << s << endl;
        e->set_i_val( i );
        break;
      }
    case 'D':
      {
        double d;
        istringstream parser(s);
        parser >> d;
        if (!parser)
          LOG(WARNING) << "parse_with_type failed for type " << typecode << " and string " << s << endl;
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
GenericParse::process(const boost::shared_ptr<Tuple> t) {
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
      LOG(FATAL) << "Regex match error thrown: " << e.what() << endl;
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
    LOG(WARNING) << "No parse matches on string: " << t->e(fld_to_parse).s_val() << endl;
    // what do we do on parse failures?  Currently, suppress silently as 'no match'
    return;
  }

  if (keep_unparsed)
    for (int i = fld_to_parse+1; i < t->e_size(); ++i) {
      Element * e = t2->add_e();
      e->CopyFrom(t->e(i));
    }  

  emit (t2);
}

void
ExtendOperator::process (boost::shared_ptr<Tuple> t) {
  
  //TODO: should we copy t first?
  for (u_int i = 0; i < new_data.size(); ++i) {
    Element * e = t->add_e();
    e->CopyFrom(new_data[i]);
  }
  emit(t);
}

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


void
SampleOperator::process (boost::shared_ptr<Tuple> t) {
  uint32_t v = gen();
  if (v >= boost::interprocess::ipcdetail::atomic_read32(&threshold)) {
    emit(t);
  }
}

operator_err_t
SampleOperator::configure (std::map<std::string,std::string> &config) {
  double frac_to_drop = 0;
  istringstream(config["fraction"]) >> frac_to_drop;
  threshold = frac_to_drop * numeric_limits<uint32_t>::max();
  int seed = 0;
  istringstream(config["seed"]) >> seed;
  gen.seed(seed);
  return NO_ERR;
}



void
HashSampleOperator::process (boost::shared_ptr<Tuple> t) {

  uint32_t hashval = 0;
  const Element& e = t->e(hash_field);
  switch(hash_type) {
    case 'I': {
      int val = e.i_val();
      hashval = jenkins_one_at_a_time_hash((char *) &val, sizeof(val));
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
  if (hashval >= boost::interprocess::ipcdetail::atomic_read32(&threshold)) {
    emit(t);
  }
  
}


operator_err_t
HashSampleOperator::configure (std::map<std::string,std::string> &config) {
  double frac_to_drop = 0;
  istringstream(config["fraction"]) >> frac_to_drop;
  threshold = frac_to_drop * numeric_limits<uint32_t>::max();

  if( !istringstream(config["hash_field"]) >> hash_field) {
    return operator_err_t("hash_field must be an int");
  }
  if(config["hash_type"].length() < 1) {
    return operator_err_t("hash_type must be defined");
  } else
    hash_type = config["hash_type"][0];
  return NO_ERR;
}


void
TRoundingOperator::process (boost::shared_ptr<Tuple> t) {
  if (in_type == T) {
    time_t old_val = t->e(fld_offset).t_val();
    t->mutable_e(fld_offset)->set_t_val((old_val / round_to) * round_to + add_offset);
  }

  // FIXME is it okay to add fixed offset _outside_ of the numeric cast?

  if (in_type == I) {
    int old_val = t->e(fld_offset).i_val();
    t->mutable_e(fld_offset)->clear_i_val();
    t->mutable_e(fld_offset)->set_t_val(numeric_cast<time_t>((old_val / round_to) * round_to) + add_offset);
  }

  if (in_type == D) {
    double old_val = t->e(fld_offset).d_val();
    t->mutable_e(fld_offset)->clear_d_val();
    t->mutable_e(fld_offset)->set_t_val(numeric_cast<time_t>(old_val / round_to) * round_to + add_offset);
  }

  emit(t);
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

    split( lines, buf, is_any_of( "\n" ) );
    
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
}

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
TimestampOperator::process (boost::shared_ptr<Tuple> t) {
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
  emit(t);
}

const string FileRead::my_type_name("FileRead operator");
const string CSVParse::my_type_name("CSVParse operator");
const string StringGrep::my_type_name("StringGrep operator");
const string GenericParse::my_type_name("Parser operator");
const string ExtendOperator::my_type_name("Extend operator");
const string TimestampOperator::my_type_name("Timestamp operator");
const string OrderingOperator::my_type_name("Ordering operator");

const string SampleOperator::my_type_name("Sample operator");
const string HashSampleOperator::my_type_name("Hash-sample operator");
const string TRoundingOperator::my_type_name("Time rounding");
const string UnixOperator::my_type_name("Unix command");


}
