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
  map<string,string> config;
  config["file"] =  "src/tests/data/base_operators_data.txt";
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  reader.set_dest(rec);
  reader.start(config);
  // Wait for reader to process entire file (alternatively, call stop() after a while)
  while (reader.isRunning()) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(200));
  }

  ASSERT_GT(rec->tuples.size(), (size_t)4);
  string s = rec->tuples[0].e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.
}


TEST(Operator, GrepOperator)
{
  map<string,string> config;
  config["pattern"] = "/usr";
  config["id"] = "1";
  
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  shared_ptr<StringGrep> grepper(new StringGrep);
  grepper->set_dest(rec);
  grepper->start(config);

  {
    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("foo");
    t->add_e()->set_s_val("/usr/bar"); //should match
    grepper->process(t);
  }

  ASSERT_EQ((size_t)1, rec->tuples.size());
  
  {
    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("/user/foo");
    t->add_e()->set_s_val("/var/bar"); //should NOT match
    grepper->process(t);
  }
  ASSERT_EQ((size_t)1, rec->tuples.size());

  {
    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("foo");
    t->add_e()->set_s_val("/var/usr/bar"); //should match
    grepper->process(t);
  }

  ASSERT_EQ((size_t)2, rec->tuples.size());
}


TEST(Operator, OperatorChain)
{
  boost::regex re1, re2;
  // For convenience, use the same config table for multiple operators
  map<string,string> config;

  // Create a chain of operators that reads lines from a file, filters them, and 
  // stores the results
  FileRead reader;
  shared_ptr<StringGrep> grepper1(new StringGrep);
  shared_ptr<StringGrep> grepper2(new StringGrep);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  grepper2->set_dest(rec);
  grepper1->set_dest(grepper2);
  reader.set_dest(grepper1);
  config["pattern"] = "/usr";
  config["id"] = "0";
  re1.assign(config["pattern"]);
  grepper2->start(config);
  // Filters comments, i.e. any line that has a '#' in it
  config["pattern"] = "^((?!#).)*$";
  re2.assign(config["pattern"]);
  grepper1->start(config);
  config["file"] = "src/tests/data/base_operators_data.txt";
  reader.start(config);
  // Wait for reader to process entire file (alternatively, call stop() after a while)
  while (reader.isRunning()) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(200));
  }

  ASSERT_GT(rec->tuples.size(), (size_t)4);
  // Each string should match both grep patterns, since they are in series
  boost::smatch matchResults;
  bool match;
  for (vector<Tuple>::iterator it = rec->tuples.begin(); it != rec->tuples.end(); it++) {
    match = boost::regex_search(it->e(0).s_val(), matchResults, re1);
    match = (match && boost::regex_search(it->e(0).s_val(), matchResults, re2));
    ASSERT_TRUE(match);
  }
}

