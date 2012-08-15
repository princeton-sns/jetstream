/**
*  Tests for various system operators, such as ReadLine
*/

#include "operators.h"
#include <map>
#include <iostream>
#include <gtest/gtest.h>


using namespace jetstream;
using namespace boost;

class DummyReceiver: public Receiver {
 public:
  vector<Tuple> tuples;
  virtual void process(shared_ptr<Tuple> t) {
    tuples.push_back(*t);
  }
};


TEST(Operator, ReadOperator) {
  FileReadOperator reader;
  map<string,string> config;
  config["file"] =  "/etc/shells";
  DummyReceiver rec;
  reader.set_dest(&rec);
  reader.start(config);
  
  ASSERT_GT(rec.tuples.size(), 4);
  
/*  for(vector<Tuple>::iterator it = rec.tuples.begin() ; it != rec.tuples.end(); ++it) {
    Tuple t = *it;
    cout <<t;
  }*/
}