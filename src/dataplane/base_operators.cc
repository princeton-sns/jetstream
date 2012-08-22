#include "dataplaneoperator.h"
#include "operators.h"
#include <iostream>
#include <fstream>
#include "stdlib.h"

#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>


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
  boost::thread loop_thread = boost::thread(*this);
}

  
void
FileRead::operator()() {
  
  ifstream in_file (f_name.c_str());
  string line;
  while (running && !in_file.eof()) {
    getline(in_file, line);
    shared_ptr<Tuple> t( new Tuple);
    Element * e = t->add_e();
    e->set_s_val(line);
    
    emit(t);
  }
}

void
StringGrep::start(map<string,string> config) {
  string pattern = config["pattern"];
  istringstream ( config["id"] ) >> id;
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
    cout << "received empty tuple, ignoring"<< endl;
    return;
  }
  //TODO: Assuming its the first element for now
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

}
