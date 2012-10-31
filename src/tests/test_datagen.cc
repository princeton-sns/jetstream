#include <iostream>
#include "node.h"

#include "rand_source.h"
#include "experiment_operators.h"

#include "js_utils.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using namespace ::std;

const int compID = 4;


TEST(Operator, RandGen) {
//  Node n;
  RandSourceOperator op;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  operator_config_t cfg;
  cfg["n"] = "2";
  cfg["k"] = "0";
  cfg["tuples_per_sec"] = "1000";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  op.set_dest(rec);
  
  op.start();
  boost::this_thread::sleep(boost::posix_time::milliseconds(100));
  op.stop();
  
  cfg["k"] = "1";
  err = op.configure(cfg);
  op.set_dest(rec);
  ASSERT_EQ(NO_ERR, err);
  
  cout << "running a second time" <<endl;
  op.start();
  boost::this_thread::sleep(boost::posix_time::milliseconds(100));
  op.stop();
  
  cout << "results:" <<endl;
  map<string,int> results;
  for (unsigned int i=0; i < rec->tuples.size(); ++i) {
    shared_ptr<Tuple> t = rec->tuples.at(i);
    results[t->e(0).s_val()] += 1;
  }
  
  map<string, int >::iterator iter = results.begin();
  
  while (iter != results.end()) {
    cout << iter->first << " " << iter->second << endl;
    iter ++;
  }
  //examine results

}


TEST(Operator, GoodnessOfData) {
  RandEvalOperator op;
  
  operator_config_t cfg;
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  
  cout << "starting eval operator" <<endl;
  op.start();

  
  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple(*t, "California");
  extend_tuple_time(*t, 1);
  extend_tuple(*t, 1);
  op.process(t);

  t->mutable_e(1)->set_t_val(2);
  int total = 0;
  for (int i=0; i < rand_data_len; ++i) {
    t->mutable_e(0)->set_s_val(rand_labels[i]);
    int v = rand_data[i];
    t->mutable_e(2)->set_i_val(v);
    total += v;
    op.process(t);
  }
  
  ASSERT_EQ(op.data_in_last_window(), 1); //new data won't have taken effect
  ASSERT_EQ(op.cur_deviation(), 0.0);

  
  t->mutable_e(1)->set_t_val(3);  
  op.process(t);

  ASSERT_EQ(op.data_in_last_window(), total);
  ASSERT_GT(op.cur_deviation(), 0.9);
  

}
