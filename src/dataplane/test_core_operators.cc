/**
*  Tests for various system operators, such as ReadLine
*/

#include "operators.h"
#include <map>
#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>

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
  boost::this_thread::sleep(boost::posix_time::seconds(1));

  ASSERT_GT(rec.tuples.size(), 4);
  string s = rec.tuples[0].e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100);

}