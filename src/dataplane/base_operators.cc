#include "dataplaneoperator.h"
#include "base_operators.h"
#include <boost/algorithm/string.hpp>

#include <iostream>
#include <fstream>
#include "stdlib.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace jetstream {


void
FileRead::configure(map<string,string> config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    LOG(WARNING) << "no file to read, bailing" << endl;
    return;
  }
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
}


void
SendK::configure(std::map<std::string,std::string> config) {
  if (config["k"].length() > 0) {
    // stringstream overloads the '!' operator to check the fail or bad bit
    if (!(stringstream(config["k"]) >> k)) {
      LOG(WARNING) << "invalid number of tuples: " << config["k"] << endl;
      return;
    }
  } else {
    // Send one tuple by default
    k = 1;
  }
  send_now = config["send_now"].length() > 0;
}

void
SendK::start() {
  if (send_now) {
    (*this)();
  }
  else {
    running = true;
    loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
  }
}


void
SendK::process(boost::shared_ptr<Tuple> t) {
  LOG(ERROR) << "Should not send data to a SendK";
} 


void
SendK::stop() {
  running = false;
  LOG(INFO) << "Stopping SendK operator";
  if (running)
    loopThread->join();
}


void
SendK::operator()() {
  boost::shared_ptr<Tuple> t(new Tuple);
  t->add_e()->set_s_val("foo");
  for (u_int i = 0; i < k; i++) {
    emit(t);
  }
}


void
StringGrep::configure(map<string,string> config) {
  string pattern = config["pattern"];
  istringstream(config["id"]) >> id;
  if (pattern.length() == 0) {
    LOG(WARNING) << "no regexp pattern specified, bailing" << endl;
    return;
  }
  re.assign(pattern);
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

  Element* e = t->mutable_e(id);
  if (!e->has_s_val()) {
    LOG(WARNING) << "received tuple but element" << id << " is not string, ignoring" << endl;
    return;
  }
  boost::smatch matchResults;
  bool found = boost::regex_search(e->s_val(), matchResults, re);
  if (found) {
    // The string element matches the pattern, so push it through
    emit(t);
  }
}



void
GenericParse::configure(std::map<std::string,std::string> config) {
  string pattern = config["pattern"];
  re.assign(pattern);
  
  istringstream(config["field_to_parse"]) >> fld_to_parse;

  field_types = boost::to_upper_copy(config["types"]);
  static boost::regex re("[SDI]+");
  
  if (!regex_match(field_types, re)) {
    LOG(WARNING) << "Invalid types for regex fields; got" << field_types;
  }
  
  if (pattern.length() == 0) {
    LOG(WARNING) << "no regexp pattern specified, bailing" << endl;
    return;
  }
}

void
GenericParse::process(const boost::shared_ptr<Tuple> t) {

  shared_ptr<Tuple> t2( new Tuple);
  for(int i = 0; i < t->e_size() && i < fld_to_parse; ++i) {
    Element * e = t2->add_e();
    e->CopyFrom(t->e(i));
  }
  
  boost::smatch matchResults;
  bool found = boost::regex_match(t->e(fld_to_parse).s_val(), matchResults, re);
  if (found) {
    for (int fld = 1; fld < matchResults.size(); ++ fld) {
      string s = matchResults.str(fld);
      
      
      
    }
  }
  else {
   // what do we do on parse failures?
  }

  for(int i = fld_to_parse+1; i < t->e_size(); ++i) {
    Element * e = t2->add_e();
    e->CopyFrom(t->e(i));
  }  
  emit (t2);
}

DummyReceiver::~DummyReceiver() {
  LOG(WARNING) << "destructing dummy receiver";
}


const string FileRead::my_type_name("FileRead operator");
const string StringGrep::my_type_name("StringGrep operator");
const string GenericParse::my_type_name("Parser operator");

const string DummyReceiver::my_type_name("DummyReceiver operator");
const string SendK::my_type_name("SendK operator");

}
