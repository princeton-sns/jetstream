/**
 *  Tests for various system operators, such as ReadLine
 */

#include "base_operators.h"
#include "chain_ops.h"
#include "variable_sampling.h"
#include <map>
#include <iostream>
#include "node.h"
#include <gtest/gtest.h>

#include "experiment_operators.h"
#include "sosp_operators.h"

using namespace jetstream;
//using namespace boost;
using boost::shared_ptr;
//using namespace std;
using std::string;
using std::cout;
using std::endl;
using std::map;
using std::vector;



TEST(Operator, FixedSampleOperator) {

  FixedSampleOperator degrade_op;

  operator_config_t cfg;
  cfg["max_drops"] = "4";
  operator_err_t err = degrade_op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  

  boost::shared_ptr<CongestionPolicy> policy(new CongestionPolicy);
  boost::shared_ptr<QueueCongestionMonitor> mockCongest(new QueueCongestionMonitor(256, "dummy"));
  mockCongest->set_downstream_congestion(0.5);
  policy->set_congest_monitor(mockCongest);
  policy->add_operator(degrade_op.id());
//  policy->clear_last_action();

  degrade_op.set_congestion_policy(policy);
  degrade_op.start();

  Tuple t;
  extend_tuple(t, 2);

  for(int i = 0; i < 10; ++i) {
    bool should_emit = degrade_op.should_emit(t);
    ASSERT_EQ( should_emit, i%2 != 0);
  }
}


TEST(Operator, BlobReader) {

  boost::shared_ptr<OperatorChain> chain(new OperatorChain);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  chain->add_member();
  chain->add_member(rec);

  BlobReader reader;
  operator_config_t cfg;
  cfg["dirname"] = "src/tests/data";
  cfg["prefix"] = "base";
  cfg["ms_per_file"] = "500";
  operator_err_t err = reader.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  reader.add_chain(chain);

  reader.emit_data();
  
  ASSERT_EQ(1, rec->tuples.size());
  Tuple & t = *(rec->tuples[0]);
  ASSERT_EQ(2, t.e_size());
  ASSERT_EQ("src/tests/data/base_operators_data.txt", t.e(0).s_val());
  const string& data = t.e(1).blob();
  cout << " data size is " << data.size() << endl;
  ASSERT_LT(200, data.size()); //some data in the tuple
            
}

