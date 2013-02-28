#include <iostream>
#include <time.h>
#include <gtest/gtest.h>

#include "cube_manager.h"
#include "node.h"

#include "base_subscribers.h"
#include "latency_measure_subscriber.h"
#include "base_operators.h"
#include "experiment_operators.h"

#include "js_utils.h"
#include "timeteller.h"

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using namespace ::std;

static const char * TEST_CUBE = "test_cube";
const int compID = 4;


class SubscriberTest : public ::testing::Test {
 protected:
  Node * node;

  virtual void SetUp() {

    NodeConfig cfg;
    boost::system::error_code error;
    node = new Node(cfg, error);
    node->start();
    ASSERT_TRUE(error == 0);

    AlterTopo topo;
    topo.set_computationid(compID);

    jetstream::CubeMeta * cube_meta = topo.add_tocreate();
    cube_meta->set_name("text");

    jetstream::CubeSchema * sc = cube_meta->mutable_schema();
    cube_meta->set_name(TEST_CUBE);
    cube_meta->set_overwrite_old(true);

    jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
    dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
    dim->set_name("url");
    dim->add_tuple_indexes(0);

    dim = sc->add_dimensions();
    dim->set_type(CubeSchema_Dimension_DimensionType_TIME);
    dim->set_name("time");
    dim->add_tuple_indexes(1);

    jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
    agg->set_name("count");
    agg->set_type("count");
    agg->add_tuple_indexes(2);
    

//  cout << topo.Utf8DebugString();

    ControlMessage r;
    node->handle_alter(topo, r);
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    cout << "alter sent; cube should be present" << endl;
  }
  
  virtual void TearDown() {
    node->stop();
  }
  
  void add_tuples(shared_ptr<DataCube> cube) {

    boost::shared_ptr<Tuple> t(new Tuple);
    t->set_version(0);
    
    extend_tuple(*t, "http://foo.com");
    extend_tuple_time(*t, 10000); //long long ago
    extend_tuple(*t, 2);
    
    cout<< "Tuple:" << fmt(*t) << endl;
    cube->process(t);
    
    time_t now = time(NULL);
    for (int i = 0; i < 3; ++i) {
      boost::shared_ptr<Tuple> t(new Tuple);

      extend_tuple(*t, "http://foo.com");
      extend_tuple_time(*t, now - i);
      extend_tuple(*t, i+1);
      t->set_version(i + 1);

      cout<< "Tuple:" << fmt(*t) << endl;
      cube->process(t);
    }
  }
  
  //add a subscriber of typename subscriberName;
  // returns a pointer to the dummy operator
  shared_ptr<DummyReceiver> start_time_subscriber (const string& subscriberName,
                                                   const Tuple& query_tuple,
                                                   const string& rollupLevels = "") {
  
    AlterTopo topo;
  
    TaskMeta* task = add_operator_to_alter(topo, operator_id_t(compID, 1), subscriberName);

    add_cfg_to_task(task, "num_results", "100");


    // Set the result limit large enough for our tests
    
    if (rollupLevels.length() > 0) {
      add_cfg_to_task(task, "rollup_levels", rollupLevels);
    } else {
      add_cfg_to_task(task,"ts_field","1");
      add_cfg_to_task(task, "start_ts", "0");    
    }

    add_cfg_to_task(task,"slice_tuple",query_tuple.SerializeAsString());

    task = add_operator_to_alter(topo, operator_id_t(compID, 2), "DummyReceiver");

    add_edge_to_alter(topo, compID, 1,2);
    add_edge_to_alter(topo, TEST_CUBE, operator_id_t(compID, 1));
    
    
    ControlMessage r;
    node->handle_alter(topo, r);
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    
    shared_ptr<DataPlaneOperator> dest = node->get_operator( operator_id_t(compID, 2));
    shared_ptr<DummyReceiver> rec = boost::static_pointer_cast<DummyReceiver>(dest);
    return rec;
  }
  
};

TEST(TimeTellerTest, TellsNormalTimeCorrectly) {
  TimeTeller t;

  int N = 1000;
  while (N-- > 0)
    EXPECT_EQ(t.now(), time(NULL));
}

TEST(TimeTellerTest, TellsSimulatedTimeCorrectly) {
  enum {D_PER_Y = 365, S_PER_D = 86400, MICROS_PER_S = 1000000};
  const time_t start_1984 = 44178120000;
  const int y_per_s = D_PER_Y * S_PER_D;
  int nSeconds = 3, iIter = -1;
  TimeSimulator t(start_1984, y_per_s);
  while (iIter++ < nSeconds) {
    time_t sim_time = start_1984 + y_per_s * iIter;
    EXPECT_EQ(sim_time, t.now());
    js_usleep(MICROS_PER_S);
  }
}

TEST_F(SubscriberTest,TimeSubscriber) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);

  add_tuples(cube);
  int tries = 0;
  while (cube->num_leaf_cells() < 4 && tries++ < 50)
    js_usleep(100 * 1000);
  
  ASSERT_EQ(4U, cube->num_leaf_cells());
  //create subscriber

  Tuple query_tuple;
  extend_tuple(query_tuple, "http://foo.com");
  extend_tuple_time(query_tuple, 0); //just a placeholder


  shared_ptr<DummyReceiver> rec = start_time_subscriber("TimeBasedSubscriber", query_tuple);
  cout << "subscriber started" << endl;
  
  tries = 0;
  while (rec->tuples.size() < 4 && tries++ < 50)
    js_usleep(100 * 1000);
  
  ASSERT_EQ(4U, rec->tuples.size());  // one very old, three newish
  
  //add more, wait and check for data
  boost::shared_ptr<Tuple> t(new Tuple);

  extend_tuple(*t, "http://foo.com");
  extend_tuple_time(*t, time(NULL));
  extend_tuple(*t, 2);
  t->set_version(0);
  cout<< "Tuple:" << fmt(*t) << endl;
  cube->process(t);

  js_usleep(1000* 1000);
  
  while (rec->tuples.size() < 5 && tries++ < 50)
    js_usleep(100 * 1000);

  ASSERT_EQ(5U, rec->tuples.size()); //update to old tuple should be suppressed
  ASSERT_TRUE(rec->tuples[0]->has_version());
  
  cout << "done" <<endl;
}

TEST_F(SubscriberTest,OneShot) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);

  add_tuples(cube);

  int tries = 0;
  for (tries = 0; cube->num_leaf_cells() < 4 && tries< 50; tries++)
    js_usleep(100 * 1000);
  ASSERT_EQ(4U, cube->num_leaf_cells());

  Tuple query_tuple;
  extend_tuple(query_tuple, "http://foo.com");
  query_tuple.add_e();  

  shared_ptr<DummyReceiver> rec = start_time_subscriber("OneShotSubscriber", query_tuple);
  cout << "subscriber started" << endl;
  
  for (tries = 0; rec->tuples.size() < 4 && tries< 50; tries++)
    js_usleep(100 * 1000);
  
  ASSERT_EQ(4U, rec->tuples.size());

  for (tries = 0; node->operator_count() > 1 && tries< 50; tries++)
    js_usleep(100 * 1000);
  ASSERT_EQ(1U, node->operator_count()); //operator should be stopped
  cout << "operator has been removed from table" << endl;
  js_usleep(500 * 1000); //put here to debug whether d-tor is called. Can remove if need be.
}


TEST_F(SubscriberTest,TimeSubscriberRollup) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);

  add_tuples(cube);

  int tries = 0;
  while (cube->num_leaf_cells() < 4 && tries++ < 50)
    js_usleep(100 * 1000);

  ASSERT_EQ(4U, cube->num_leaf_cells());
  //create subscriber
    cout << "now checking rollups" << endl;

  string rollup_levels = "1,0"; //roll up time

  Tuple query_tuple;
  extend_tuple(query_tuple, "http://foo.com");
  query_tuple.add_e();  
  shared_ptr<DummyReceiver> rec = start_time_subscriber("TimeBasedSubscriber", query_tuple, rollup_levels);
  cout << "subscriber started" << endl;
  
  tries = 0;
  while (rec->tuples.size() < 1 && tries++ < 50)
    js_usleep(100 * 1000);

  ASSERT_EQ(1U, rec->tuples.size());
//  cout << "Tuple: " << fmt( *(rec->tuples[0])) << endl;
  ASSERT_EQ(8, rec->tuples[0]->e(2).i_val());
  
/*
  {
    boost::shared_ptr<Tuple> t2(new Tuple);
    extend_tuple(*t2, "new text");
    extend_tuple(*t2, 5);
    t2->set_version(1);
    cube->process(t2);
    for(int i =0; i < 20 &&  cube->num_leaf_cells() < 2; i++) {
      js_usleep(100 * 1000);
    }
  }*/

}

TEST(LatencyMeasureSubscriber,TwoTuples) {
  const int NUM_TUPLES = 4;
  LatencyMeasureSubscriber  sub;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  sub.set_dest(rec);
  operator_config_t cfg;
  cfg["time_tuple_index"] = "0";
  cfg["hostname_tuple_index"] = "1";
  cfg["interval_ms"] = "500";
  sub.configure(cfg);
  sub.start();
  
  msec_t cur_time = get_msec();
  
  std::string hostnames[] = {"host1", "host2"};
  boost::shared_ptr<Tuple> tuples[NUM_TUPLES];
  
  for(int i=0; i < NUM_TUPLES; ++i) {
    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple(*t, double(cur_time + 100 * i));
    extend_tuple(*t, hostnames[i%2]);
    sub.action_on_tuple(t);
    tuples[i] = t;
  }
  
  js_usleep(500 * 1000);

  boost::shared_ptr<Tuple> no_tuple;
  for(int i=0; i < NUM_TUPLES; ++i) {
    sub.post_insert(tuples[i], no_tuple);
  }
  js_usleep(1500 * 1000);
  
  int count = rec->tuples.size();
  for (int i = 0; i < count; ++i) {
    cout << fmt( *(rec->tuples[i]) ) << endl;
  }
  sub.stop();
}



TEST_F(SubscriberTest,VariableSubscriber) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);
  ASSERT_EQ(2, cube->get_schema().dimensions_size());
  //schema is URL, time, count
  
    AlterTopo topo;
    Tuple query_tuple = cube->empty_tuple();
    TaskMeta* subsc = add_operator_to_alter(topo, operator_id_t(compID, 1), "VariableCoarseningSubscriber");
    add_cfg_to_task(subsc, "num_results", "100");
    add_cfg_to_task(subsc, "ts_field","1");
    add_cfg_to_task(subsc,"slice_tuple",query_tuple.SerializeAsString());

    TaskMeta* recv =  add_operator_to_alter(topo, operator_id_t(compID, 2), "FixedRateQueue");
    add_cfg_to_task(recv, "ms_wait", "300"); //just under 4 dequeues per second
    add_operator_to_alter(topo, operator_id_t(compID, 3), "DummyReceiver");

    add_edge_to_alter(topo, TEST_CUBE, operator_id_t(compID, 1));
    add_edge_to_alter(topo, compID, 1,2);
    add_edge_to_alter(topo, compID, 2,3);
    
    ControlMessage r;
    node->handle_alter(topo, r);
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    
  shared_ptr<DummyReceiver> receiver = boost::static_pointer_cast<DummyReceiver>(
  node->get_operator( operator_id_t(compID, 3)));

  shared_ptr<VariableCoarseningSubscriber> subscriber = boost::static_pointer_cast<VariableCoarseningSubscriber>(
  node->get_operator( operator_id_t(compID, 1)));


  const int URL_COUNT = 10;
  string urls[URL_COUNT];
  for (int i =0; i < URL_COUNT; ++i)
    urls[i] = "url" + boost::lexical_cast<string>(i);
  
  for (int t= 0; t < 35; ++t) {
    time_t now = time(NULL);
    for( int i =0; i < URL_COUNT; ++i) {
      boost::shared_ptr<Tuple> tuple(new Tuple);
      extend_tuple(*tuple, urls[i]);
      extend_tuple_time(*tuple, now);
      extend_tuple(*tuple, 1);
      tuple->set_version(0);
      cube->process(tuple);
    }
    js_usleep(1000 * 1000);
    if (t % 2 == 0)
      cout << "tick" << endl;
  }
  cout << "total of " << receiver->tuples.size() << " tuples received"<< endl;
  EXPECT_EQ(10000, subscriber->window_size());
//  for (int i = 0; i < receiver->tuples.size(); ++i)
//    cout << fmt( (*receiver->tuples[i])) << endl;
//    js_usleep(50 * 1000);
//subscriber, attached to a limited-rate sender, attached to a dest.
  

}
