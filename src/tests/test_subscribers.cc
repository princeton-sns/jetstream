#include <iostream>
#include <time.h>
#include <gtest/gtest.h>

#include "cube_manager.h"
#include "node.h"

#include "base_subscribers.h"
#include "latency_measure_subscriber.h"
#include "filter_subscriber.h"

#include "chain_ops.h"
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
    cfg.thread_pool_size = 3;
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
    cube->process(NULL, t);
    
    time_t now = time(NULL);
    for (int i = 0; i < 3; ++i) {
      boost::shared_ptr<Tuple> t(new Tuple);

      extend_tuple(*t, "http://foo.com");
      extend_tuple_time(*t, now - i);
      extend_tuple(*t, i+1);
      t->set_version(i + 1);

      cout<< "Tuple:" << fmt(*t) << endl;
      cube->process(NULL, t);
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
    
    shared_ptr<COperator> dest = node->get_operator( operator_id_t(compID, 2));
    shared_ptr<DummyReceiver> rec = boost::dynamic_pointer_cast<DummyReceiver>(dest);
    return rec;
  }
  

/*
  USELESS since sendk doesn't match schema
 shared_ptr<SendK>  add_send_k() {
    AlterTopo topo;
    operator_id_t oper_id(compID, 17);
    //TaskMeta* task =
    add_operator_to_alter(topo, oper_id, "SendK");
    add_edge_to_alter(topo, oper_id, TEST_CUBE);
   
    ControlMessage r;
    node->handle_alter(topo, r);
    EXPECT_NE(r.type(), ControlMessage::ERROR);

    shared_ptr<COperator> dest = node->get_operator( operator_id_t(compID, 2));
    shared_ptr<SendK> rec = boost::dynamic_pointer_cast<SendK>(dest);
    return rec;
  }
  */
  
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
  
  ASSERT_EQ(4U, cube->num_leaf_cells());  //one tuple from long ago, three from "now"
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
  cube->process(NULL, t);

  js_usleep(1000* 1000);
  
  while (rec->tuples.size() < 5 && tries++ < 50)
    js_usleep(100 * 1000);

  ASSERT_EQ(5U, rec->tuples.size()); //update to old tuple should be suppressed
  ASSERT_TRUE(rec->tuples[0]->has_version());
  
  cout << "done" <<endl;
}



TEST_F(SubscriberTest,TimeSubscriberLowLatency) {
    shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);
    
    add_tuples(cube);
    int tries = 0;
    while (cube->num_leaf_cells() < 4 && tries++ < 50)
        js_usleep(100 * 1000);
    
    ASSERT_EQ(4U, cube->num_leaf_cells());// one very old, three newish
    //create subscriber
    
    Tuple query_tuple;
    extend_tuple(query_tuple, "http://foo.com");
    extend_tuple_time(query_tuple, 0); //just a placeholder
    
    AlterTopo topo;
    TaskMeta* subsc = add_operator_to_alter(topo, operator_id_t(compID, 1), "TimeBasedSubscriber");
    add_cfg_to_task(subsc, "ts_field","1");
    add_cfg_to_task(subsc, "start_ts", "0");
    add_cfg_to_task(subsc, "window_offset","100000"); //100 seconds

    add_cfg_to_task(subsc,"slice_tuple",query_tuple.SerializeAsString());
    
    add_operator_to_alter(topo, operator_id_t(compID, 2), "DummyReceiver");
    
    add_edge_to_alter(topo, TEST_CUBE, operator_id_t(compID, 1));
    add_edge_to_alter(topo, compID, 1,2);
    
    ControlMessage r;
    node->handle_alter(topo, r);
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    
    shared_ptr<DummyReceiver> rec = boost::dynamic_pointer_cast<DummyReceiver>(
            node->get_operator( operator_id_t(compID, 2)));
    

    js_usleep(2000 * 1000);
    ASSERT_EQ(1U, rec->tuples.size()); //newish tuples won't have appeared yet
    
    DataplaneMessage window_marker;
    window_marker.set_type(DataplaneMessage::END_OF_WINDOW);
    window_marker.set_timestamp( usec_t(1000 * 1000) * time(NULL));
    vector< boost::shared_ptr<Tuple> > no_tuples;
    OperatorChain chain;
    cube->process(&chain, no_tuples, window_marker);
        
    js_usleep(1000* 1000);
    
    while (rec->tuples.size() < 4 && tries++ < 50)
        js_usleep(100 * 1000);
    
    ASSERT_EQ(4U, rec->tuples.size());     
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
  query_tuple.add_e();
  query_tuple.add_e();  

  shared_ptr<DummyReceiver> rec = start_time_subscriber("OneShotSubscriber", query_tuple);
  cout << "subscriber started" << endl;
  
  for (tries = 0; rec->tuples.size() < 4 && tries< 50; tries++)
    js_usleep(100 * 1000);
  
  ASSERT_EQ(4U, rec->tuples.size());

  for (tries = 0; node->operator_count() > 1 && tries< 50; tries++)
    js_usleep(100 * 1000);
  EXPECT_EQ(1U, node->operator_count());
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

}



TEST_F(SubscriberTest,FilterSubscriber) {

  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);

  Tuple query_tuple;
  extend_tuple(query_tuple, "http://foo.com");
  query_tuple.add_e();  

  shared_ptr<DummyReceiver> rec = start_time_subscriber("FilterSubscriber", query_tuple);

  add_tuples(cube);
  for (int tries = 0; cube->num_leaf_cells() < 4 && tries< 50; tries++)
    js_usleep(100 * 1000);
  ASSERT_EQ(4U, cube->num_leaf_cells());
  ASSERT_EQ(4U, rec->tuples.size());
}

TEST(LatencyMeasureSubscriber,TwoTuples) {
  const int NUM_TUPLES = 4;
  shared_ptr<LatencyMeasureSubscriber> sub(new LatencyMeasureSubscriber);
  shared_ptr<DummyReceiver> rec(new DummyReceiver);
  shared_ptr<OperatorChain> chain(new OperatorChain);
  chain->add_member(sub);
  chain->add_member(rec);
  sub->add_chain(chain);

  operator_config_t cfg;
  cfg["time_tuple_index"] = "0";
  cfg["hostname_tuple_index"] = "1";
  cfg["interval_ms"] = "500";
  sub->configure(cfg);
  sub->start();
  
  msec_t cur_time = get_msec();
  
  std::string hostnames[] = {"host1", "host2"};
  boost::shared_ptr<Tuple> tuples[NUM_TUPLES];
  
  for(int i=0; i < NUM_TUPLES; ++i) {
    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple(*t, double(cur_time + 100 * i));
    extend_tuple(*t, hostnames[i%2]);
    sub->action_on_tuple(NULL, t);
    tuples[i] = t;
  }
  
  js_usleep(500 * 1000);

  boost::shared_ptr<Tuple> no_tuple;
  for(int i=0; i < NUM_TUPLES; ++i) {
    sub->post_insert(tuples[i], no_tuple);
  }
  js_usleep(1500 * 1000);
  
  int count = rec->tuples.size();
  for (int i = 0; i < count; ++i) {
    cout << fmt( *(rec->tuples[i]) ) << endl;
  }
  chain->stop();
}



TEST_F(SubscriberTest,VariableSubscriber) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);
  ASSERT_EQ(2, cube->get_schema().dimensions_size());
  //schema is URL, time, count
  
    AlterTopo topo;
    Tuple query_tuple = cube->empty_tuple();
    TaskMeta* subsc = add_operator_to_alter(topo, operator_id_t(compID, 1), "VariableCoarseningSubscriber");
    add_cfg_to_task(subsc, "num_results", "100");
//    add_cfg_to_task(subsc, "ts_field","1");
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
    
  shared_ptr<DummyReceiver> receiver = boost::dynamic_pointer_cast<DummyReceiver>(
  node->get_operator( operator_id_t(compID, 3)));

  shared_ptr<VariableCoarseningSubscriber> subscriber = boost::dynamic_pointer_cast<VariableCoarseningSubscriber>(
  node->get_operator( operator_id_t(compID, 1)));


  const int URL_COUNT = 18; //should take more than 5s to send 'em all
  string urls[URL_COUNT];
  for (int i =0; i < URL_COUNT; ++i)
    urls[i] = "url" + boost::lexical_cast<string>(i);
  
    //preload some data into cube
  time_t now = time(NULL);
  for( int i =0; i < URL_COUNT; ++i) {
    boost::shared_ptr<Tuple> tuple(new Tuple);
    extend_tuple(*tuple, urls[i]);
    extend_tuple_time(*tuple, now);
    extend_tuple(*tuple, 1);
    tuple->set_version(0);
    cube->process(NULL, tuple);
  }
  
  size_t tuples_last_window = 0;
  for (int t= 0; t < 10; ++t) {
      //cube will effectively just do the same query over and over
    js_usleep(2000 * 1000);

    size_t s = receiver->tuples.size();
    cout << "tick; " <<  (s - tuples_last_window) << " tuples." << endl;
    tuples_last_window = s;
  }
  cout << "total of " << receiver->tuples.size() << " tuples received"<< endl;
  EXPECT_GE(10000, subscriber->window_size());
  EXPECT_LE(5000, subscriber->window_size());

}



TEST_F(SubscriberTest,DelayedOneShot) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);

  Tuple query_tuple;
  query_tuple.add_e();  
  query_tuple.add_e();

  
  shared_ptr<DummyReceiver> rec = start_time_subscriber("DelayedOneShotSubscriber", query_tuple);
  cout << "subscriber started" << endl;
  ASSERT_EQ(0U, rec->tuples.size());

//  boost::shared_ptr<OperatorChain>  fakeChain(new OperatorChain);
  shared_ptr<Tuple> t(new Tuple);
  t->set_version(0);
  extend_tuple(*t, "http://foo.com");
  extend_tuple_time(*t, 10000); //long long ago
  extend_tuple(*t, 2);
  
  OperatorChain fakeChain;
  cube->process(&fakeChain, t);

  js_usleep(1000 * 1000);
  ASSERT_EQ(0U, rec->tuples.size());

  vector< shared_ptr<Tuple> > no_tuples;
  DataplaneMessage end_marker;
  end_marker.set_type(DataplaneMessage::NO_MORE_DATA);
  cube->process(&fakeChain, no_tuples, end_marker);
  
  js_usleep(100 * 1000);
  ASSERT_EQ(1, rec->tuples.size());
  EXPECT_EQ(1U, node->operator_count());


}
