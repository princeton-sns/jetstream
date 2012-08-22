/**
*  Tests for various system operators, such as ReadLine
*/

#include "operators.h"
#include <map>
#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>
#include <boost/regex.hpp>

#include <gtest/gtest.h>


using namespace jetstream;
using namespace boost;
using namespace std;


TEST(Operator, ReadOperator) {
  FileRead reader;
  boost::regex re;
  // Intentionally using the same config table for multiple operators
  map<string,string> config;
  config["file"] =  "/etc/shells";
  config["pattern"] = "/usr";
  re.assign(config["pattern"]);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  shared_ptr<StringGrep> grepper(new StringGrep);
  reader.set_dest(grepper);
  grepper->set_dest(rec);
  grepper->start(config);
  reader.start(config);
  boost::this_thread::sleep(boost::posix_time::seconds(1));

  ASSERT_GT(rec->tuples.size(),(unsigned int) 4);
  string s = rec->tuples[0].e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() - 1], '\n'); //check that we prune \n.
  // Check if all strings match the pattern
  boost::smatch matchResults;
  bool found;
  for (vector<Tuple>::iterator it = rec->tuples.begin(); it != rec->tuples.end(); it++) {
    found = boost::regex_search(it->e(0).s_val(), matchResults, re);
    ASSERT_TRUE(found);
  }
}
