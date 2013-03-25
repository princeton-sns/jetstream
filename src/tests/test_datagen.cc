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
  for (unsigned int i=0; i < op.rand_data.size(); ++i) {
    t->mutable_e(0)->set_s_val(op.rand_labels[i]);
    int v = op.rand_data[i];
    t->mutable_e(2)->set_i_val(v);
    total += v;
    op.process(t);
  }
  
  ASSERT_EQ(1, op.data_in_last_window()); //new data won't have taken effect
  ASSERT_EQ(0.0, op.cur_deviation());

  
  t->mutable_e(1)->set_t_val(3);  
  op.process(t);

  ASSERT_EQ(op.data_in_last_window(), total);
  ASSERT_GT(op.cur_deviation(), 0.9);
}

shared_ptr<RandEvalOperator> src_eval_pair(operator_config_t cfg, int BATCHES) {

  shared_ptr<RandEvalOperator> eval(new RandEvalOperator);
  shared_ptr<DummyReceiver> receiver(new DummyReceiver);

  eval->configure(cfg);

  RandSourceOperator op;
  cfg["n"] = "1";
  cfg["k"] = "0";
  operator_err_t err = op.configure(cfg);

  shared_ptr<DataPlaneOperator> extend(new ExtendOperator);
  cfg["types"] = "i";
  cfg["0"] = "1";
  extend->configure(cfg);  //add a dummy count

  cfg.clear();
  
  EXPECT_EQ(NO_ERR, err);


  op.set_dest(extend);
  extend->set_dest(receiver);
  cout << "starting source" << endl;
  for (int i =0; i < BATCHES; ++i) {
    op.emit_1();
    cout << "after batch " << i << " have " << receiver->tuples.size() << " tuples" << endl;
   }
//  op.start();
//  boost::this_thread::sleep(boost::posix_time::milliseconds(700));
//  op.stop();
  
  unsigned int total = op.emitted_count();
  cout << "source stopped after emitting " << total << endl;
  EXPECT_EQ(total, (unsigned int)extend->emitted_count());

  EXPECT_EQ(total, receiver->tuples.size());
  time_t now = time(NULL);
  for (unsigned int i = 0; i < receiver->tuples.size(); ++i){
    boost::shared_ptr<Tuple> t = receiver->tuples[i];
    t->mutable_e(1)->set_t_val(now);
    eval->process(t);
  }
  
  

  shared_ptr<Tuple> t = shared_ptr<Tuple>(new Tuple);
  extend_tuple(*t, "California");
  extend_tuple_time(*t, time(NULL) + 1);
  extend_tuple(*t, 1);
  eval->process(t);

  EXPECT_EQ(eval->data_in_last_window(), total);
  EXPECT_GT(eval->cur_deviation(), 0.9);
  return eval;
}

TEST(Operator, RandSourceIntegration_S) {
  operator_config_t cfg;
  cfg["rate"] = "500";

  shared_ptr<RandEvalOperator> rec = src_eval_pair(cfg, 1);
}

TEST(Operator, RandSourceIntegration_Z) {
  operator_config_t cfg;
  cfg["mode"] = "zipf";
  cfg["rate"] = "2000"; 
  cfg["items"] = "50"; // tail of zipf won't validate, so we truncate at 12
  shared_ptr<RandEvalOperator> rec = src_eval_pair(cfg, 4);
  // can do more validation here if need be
}
