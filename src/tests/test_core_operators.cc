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
using namespace std;


TEST(Operator, ReadOperator) {
  FileRead reader;
  map<string,string> config;
  config["file"] =  "/etc/shells";
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  reader.set_dest(rec);
  reader.start(config);
  boost::this_thread::sleep(boost::posix_time::seconds(1));

  ASSERT_GT(rec->tuples.size(),(unsigned int) 4);
  string s = rec->tuples[0].e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.

}
