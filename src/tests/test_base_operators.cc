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
  config["file"] =  "/etc/shells";
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  reader.set_dest(rec);
  reader.start(config);
  boost::this_thread::sleep(boost::posix_time::seconds(1));

  ASSERT_GT(rec->tuples.size(), 4);
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
    boost::shared_ptr<Tuple> t1(new Tuple);
    t1->add_e()->set_s_val("foo");
    t1->add_e()->set_s_val("/usr/bar"); //should match
    grepper->process(t1);
  }

  ASSERT_EQ(1, rec->tuples.size());
  
  {
    boost::shared_ptr<Tuple> t(new Tuple);
    
    t->add_e()->set_s_val("/user/foo");
    t->add_e()->set_s_val("/var/bar"); //should NOT match
    grepper->process(t);
  }
  ASSERT_EQ(1, rec->tuples.size());

  {
    boost::shared_ptr<Tuple> t(new Tuple);
    t->add_e()->set_s_val("foo");
    t->add_e()->set_s_val("/var/usr/bar"); //should NOT match:
                            //match starts at start of field
    grepper->process(t);
  }

  ASSERT_EQ(2, rec->tuples.size());
  

}