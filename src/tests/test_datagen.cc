#include <iostream>
#include "node.h"

#include "rand_source.h"
#include "experiment_operators.h"
#include "base_operators.h"


#include "js_utils.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using namespace ::std;

const int compID = 4;


TEST(Operator, RandGenHalf) {
//  Node n;
  RandSourceOperator op;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  operator_config_t cfg;
  cfg["n"] = "2";
  cfg["k"] = "0";
  cfg["rate"] = "500";
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
  //TODO: examine results

}


TEST(Operator, RandGenFull) {
//  Node n;
  RandSourceOperator op;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  operator_config_t cfg;
  cfg["n"] = "1";
  cfg["k"] = "0";
  cfg["rate"] = "2000";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);
  op.set_dest(rec);
  
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
  //TODO: examine results

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

TEST(Operator, RandSourceIntegration) {
  shared_ptr<RandEvalOperator> rec(new RandEvalOperator);
  operator_config_t cfg;
  rec->configure(cfg);

  shared_ptr<DataPlaneOperator> extend(new ExtendOperator);
  cfg["types"] = "i";
  cfg["0"] = "1";
  extend->configure(cfg);

  cfg.clear();
  
  RandSourceOperator op;
  cfg["n"] = "1";
  cfg["k"] = "0";
  cfg["rate"] = "1000";
  operator_err_t err = op.configure(cfg);
  ASSERT_EQ(NO_ERR, err);


  op.set_dest(extend);
  extend->set_dest(rec);
  
  cout << "starting source" << endl;
  op.start();
  boost::this_thread::sleep(boost::posix_time::milliseconds(100));
  op.stop();
  
  cout << "source stopped" << endl;

  int total = op.emitted_count();


  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple(*t, "California");
  extend_tuple_time(*t, time(NULL) + 1);
  extend_tuple(*t, 1);
  rec->process(t);

  ASSERT_EQ(rec->data_in_last_window(), total);
  ASSERT_GT(rec->cur_deviation(), 0.9);

}