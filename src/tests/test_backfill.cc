#include <gtest/gtest.h>
#include <glog/logging.h>

#include "js_utils.h"

#include "experiment_operators.h"
#include "base_operators.h"

using namespace ::std;
using namespace boost;
using namespace jetstream;


TEST(Backfill, OperatorChain) {

  ExtendOperator ex_1;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);

  operator_config_t cfg;
  cfg["types"] = "i";
  cfg["0"] = "1";
  ex_1.configure(cfg);
  ex_1.set_dest(rec);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  ex_1.process(t);

  ASSERT_EQ((size_t)1, rec->tuples.size());
  ASSERT_EQ(2, rec->tuples[0]->e_size()); //buffer holds (2,1)

  t->clear_e();
  extend_tuple(*t, 2);

  boost::shared_ptr<Tuple> t2(new Tuple);
  extend_tuple(*t2, 5);
  operator_id_t no_id;
  ex_1.process_delta(*t, t2, no_id);

  ASSERT_EQ((size_t)1, rec->tuples.size());
  ASSERT_EQ(2, rec->tuples[0]->e_size());
  ASSERT_EQ(5, rec->tuples[0]->e(0).i_val());
}

