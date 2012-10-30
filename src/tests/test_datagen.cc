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
  for (int i=0; i < rec->tuples.size(); ++i) {
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