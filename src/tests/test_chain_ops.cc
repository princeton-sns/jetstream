#include <gtest/gtest.h>
#include <glog/logging.h>

#include "operator_chain.h"
#include "chain_ops.h"
#include "node.h"

using namespace ::std;
using namespace boost;
using namespace jetstream;


class COperatorTest : public ::testing::Test {
public:

  boost::shared_ptr<Node> node;
  virtual void SetUp() {
  
    if (!node) {
      NodeConfig cfg;
      cfg.heartbeat_time = 2000;
      boost::system::error_code err;
      node = shared_ptr<Node>(new Node(cfg, err));
    }
    node->start();
  }
  
  virtual void TearDown() {
    node->stop();
  }


};


TEST_F(COperatorTest, FileRead) {
  // constants describing the test data file
  enum {TEST_DATA_N_LINES = 19, TEST_DATA_N_EMPTY = 1};

  OperatorChain chain;
  shared_ptr<CFileRead> reader(new CFileRead);
  map<string,string> config;
  config["file"] =  "src/tests/data/base_operators_data.txt";
  config["skip_empty"] = "false";
  config["exit_at_end"] = "true";
  reader->configure(config);
  reader->set_node(node.get());
  
  shared_ptr<CDummyReceiver> rec(new CDummyReceiver);
  
  chain.add_operator(reader);
  chain.add_operator(rec);
  
  chain.start();
  
  // Wait for reader to process entire file (alternatively, call stop() after a
  // while)
  boost::this_thread::sleep(boost::posix_time::milliseconds(200));

  /*
  int waits = 0;
  while (reader.isRunning() && waits++ < 20) {
    boost::this_thread::sleep(boost::posix_time::milliseconds(200));
  }
  ASSERT_GT (20, waits);*/
  chain.stop();

  ASSERT_GT(rec->tuples.size(), (size_t)4);
  ASSERT_EQ((size_t) TEST_DATA_N_LINES + 1, rec->tuples.size()); // file read adds blank line at end of file
  string s = rec->tuples[0]->e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.


  // try again, with the option to skip 0-length lines turned on
  rec->tuples.clear();

  shared_ptr<CFileRead> reader2(new CFileRead);
  config["skip_empty"] = "true";
  reader2->configure(config);
  reader2->set_node(node.get());
  
  
  OperatorChain chain2;
  chain2.add_operator(reader2);
  chain2.add_operator(rec);
  
  chain2.start();
  boost::this_thread::sleep(boost::posix_time::milliseconds(200));

  chain2.stop();


  ASSERT_EQ((size_t) TEST_DATA_N_LINES - TEST_DATA_N_EMPTY, rec->tuples.size());
  ASSERT_GT(rec->tuples.size(), (size_t)4);
  s = rec->tuples[0]->e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
  ASSERT_NE(s[s.length() -1], '\n'); //check that we prune \n.
}
