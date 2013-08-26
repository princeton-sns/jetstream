
#include <map>
#include <iostream>
#include <gtest/gtest.h>

#include "experiment_operators.h"
#include "summary_operators.h"
#include "quantile_est.h"


using namespace jetstream;
using namespace std;


TEST(Operator, QuantileAndCountOperators) {


  OperatorChain chain;
  
  boost::shared_ptr<SummaryToCount> s2count(new SummaryToCount);
  boost::shared_ptr<QuantileOperator> q_op(new QuantileOperator);
  boost::shared_ptr<DummyReceiver> receive(new DummyReceiver);

  operator_config_t cfg;
  cfg["q"] = "0.6";
  cfg["field"] = "1";

  operator_err_t err = q_op->configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  err = s2count->configure(cfg);
  ASSERT_EQ(NO_ERR, err);

  chain.add_member();
  chain.add_member(s2count);
  chain.add_member(q_op);


  LogHistogram lh(500);
  lh.add_item(2, 1);
  lh.add_item(4, 1);
  lh.add_item(6, 1);


  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  extend_tuple(*t, lh);

  vector< boost::shared_ptr<Tuple> > tuples;
  tuples.push_back(t);
  DataplaneMessage no_meta;
  chain.process(tuples, no_meta);

  ASSERT_EQ((size_t)1, tuples.size());
  boost::shared_ptr<Tuple> result = tuples[0];
  ASSERT_EQ(2, result->e(0).i_val());  //first element preserved
  ASSERT_EQ(4, result->e(1).i_val()); //median is 4; 0.6th quantile is between 4 and 6
  ASSERT_EQ(3, result->e(2).i_val()); //three values
}


TEST(Operator, ToSummary) {
  ToSummary op;
  operator_config_t cfg;
  cfg["size"] = "50";
  cfg["field"] = "0";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);

  op.process_one(t);

  ASSERT_EQ(2, t->e(0).summary().items(0)); //only two items in the summary
}


TEST(Operator, DegradeSummary) {

  DegradeSummary degrade_op;

  operator_config_t cfg;
//  cfg["q"] = "0.6";
  cfg["field"] = "1";
  degrade_op.configure(cfg);

  boost::shared_ptr<CongestionPolicy> policy(new CongestionPolicy);
  boost::shared_ptr<QueueCongestionMonitor> mockCongest(new QueueCongestionMonitor(256, "dummy"));
  mockCongest->set_downstream_congestion(0.5, get_msec() + 10000);
  policy->set_congest_monitor(mockCongest);
  policy->add_operator(degrade_op.id());
//  policy->clear_last_action();

  degrade_op.set_congestion_policy(policy);
  degrade_op.start();


  LogHistogram lh(500);
  lh.add_item(2, 1);
  lh.add_item(4, 1);
  lh.add_item(6, 1);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  extend_tuple(*t, lh);

  degrade_op.process_one(t);
  
  ASSERT_TRUE(  t->e(1).summary().has_histo());
  int h_size = t->e(1).summary().histo().bucket_vals_size();
  ASSERT_EQ( 250 , h_size);
  
//  cout << fmt(*result) << endl;

}