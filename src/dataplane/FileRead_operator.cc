#include "dataplaneoperator.h"
#include "operators.h"
#include <iostream>
#include <fstream>

#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>


using namespace std;
using namespace boost;

namespace jetstream {

  
  
void
FileRead::start(map<string,string> config) {
  f_name = config["file"];
  if (f_name.length() == 0) {
    cout << "no file to read, bailing"<<endl;
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


}


extern "C" {
  jetstream::DataPlaneOperator *maker(){
    return new jetstream::FileRead();
  }
}