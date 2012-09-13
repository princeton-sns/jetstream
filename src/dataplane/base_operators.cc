#include "dataplaneoperator.h"
#include "operators.h"
#include <iostream>
#include <fstream>
#include "stdlib.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace jetstream {


void
FileRead::start(map<string,string> config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    cout << "no file to read, bailing" << endl;
    return;
  }
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
    cout << "could not open file " << f_name.c_str() << endl;
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
SendK::start(std::map<std::string,std::string> config) {
  if (config["k"].length() > 0) {
    // stringstream overloads the '!' operator to check the fail or bad bit
    if (!(stringstream(config["k"]) >> k)) {
      cout << "invalid number of tuples: " << config["k"] << endl;
      return;
    }
  } else {
    // Send one tuple by default
    k = 1;
  }
  running = true;
  loopThread = shared_ptr<boost::thread>(new boost::thread(boost::ref(*this)));
}


void
SendK::process(boost::shared_ptr<Tuple> t) {
  LOG(ERROR) << "Should not send data to a SendK";
} 


void
SendK::stop() {
  running = false;
  LOG(INFO) << "Stopping SendK operator";
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
StringGrep::start(map<string,string> config) {
  string pattern = config["pattern"];
  istringstream(config["id"]) >> id;
  if (pattern.length() == 0) {
    cout << "no regexp pattern specified, bailing" << endl;
    return;
  }
  re.assign(pattern);
}


void
StringGrep::process (boost::shared_ptr<Tuple> t)
{
  assert(t);
  if (re.empty()) {
    cout << "no pattern assigned; did you start the operators in the right order?" << endl;
    return;
  }
  if (t->e_size() == 0) {
    cout << "received empty tuple, ignoring" << endl;
    return;
  }

  Element* e = t->mutable_e(id);
  if (!e->has_s_val()) {
    cout << "received tuple but element" << id << " is not string, ignoring" << endl;
    return;
  }
  boost::smatch matchResults;
  bool found = boost::regex_search(e->s_val(), matchResults, re);
  if (found) {
    // The string element matches the pattern, so push it through
    emit(t);
  }
}


DummyReceiver::~DummyReceiver() {
  LOG(WARNING) << "destructing dummy receiver";
}

}
