#include "dataplaneoperator.h"
#include "base_operators.h"
#include <boost/algorithm/string.hpp>

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>

#include <glog/logging.h>
#include <boost/asio/ip/host_name.hpp>
#include <boost/algorithm/string.hpp>

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
  while (running && in_file.good()) {
    getline(in_file, line);
    shared_ptr<Tuple> t( new Tuple);
    Element * e = t->add_e();
    e->set_s_val(line);
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
  re.assign(pattern);
  
  
  istringstream(config["field_to_parse"]) >> fld_to_parse;
  if (fld_to_parse < 0 || fld_to_parse > 100) {
    LOG(WARNING) << "field ID " << fld_to_parse << "looks bogus";
  }

  field_types = boost::to_upper_copy(config["types"]);
  static boost::regex re("[SDI]+");
  
  if (!regex_match(field_types, re)) {
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
        istringstream(s) >> i;
        e->set_i_val( i );
        break;
      }
    case 'D':
      {
        double d;
        istringstream(s) >> d;
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

  shared_ptr<Tuple> t2( new Tuple);
  for(int i = 0; i < t->e_size() && i < fld_to_parse; ++i) {
    Element * e = t2->add_e();
    e->CopyFrom(t->e(i));
  }
  
  if (fld_to_parse >= t->e_size()) {
    LOG(WARNING) << "can't parse field " << fld_to_parse << "; total size is only" << t->e_size();
  }
  
  boost::smatch matchResults;
  bool found = boost::regex_match(t->e(fld_to_parse).s_val(), matchResults, re);
  if (found) {
    for (size_t fld = 1; fld < matchResults.size(); ++ fld) {
      string s = matchResults.str(fld);
      char typecode = field_types[fld-1];
      Element * e = t2->add_e();
      parse_with_types(e, s, typecode);
    }
  }
  else {
   // what do we do on parse failures?  Currently, suppress silently as 'no match'
    return;
  }

  for(int i = fld_to_parse+1; i < t->e_size(); ++i) {
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
  if (v >= threshold) {
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
TRoundingOperator::process (boost::shared_ptr<Tuple> t) {
  time_t old_val = t->e(fld_offset).t_val();
  t->mutable_e(fld_offset)->set_t_val(  (old_val / round_to) * round_to);

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
      Element * e = t->add_e();
      e->set_s_val(lines[i]);
      emit(t);
    }
  }
  return true; //we're done if we failed to read.
}


const string FileRead::my_type_name("FileRead operator");
const string StringGrep::my_type_name("StringGrep operator");
const string GenericParse::my_type_name("Parser operator");
const string ExtendOperator::my_type_name("Extend operator");
const string SampleOperator::my_type_name("Sample operator");
const string TRoundingOperator::my_type_name("Time rounding");
const string UnixOperator::my_type_name("Unix command");


}
