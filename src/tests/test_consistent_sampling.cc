
#include <iostream>

#include "js_utils.h"
#include "experiment_operators.h"

#include "queue_congestion_mon.h"
#include "variable_sampling.h"

#include "node.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;

const int compID = 4;


TEST(Sampling, TwoLocalChains) {
  const int CHAINS = 2;
  int K = 100;
  ostringstream fmt;
  fmt << K;
  string k_as_str = fmt.str();

  int nextOpID = 1;
  int compID = 1;

  operator_id_t chainHeadIDs[CHAINS], mockCongestID[CHAINS];
  operator_id_t receiver_id(compID, nextOpID++);
  operator_id_t orderer_id(compID, nextOpID++);

  operator_id_t c_controller_id(compID, nextOpID++);
  
  
  
  AlterTopo topo;  
  
  add_operator_to_alter(topo, receiver_id, "DummyReceiver");
  add_operator_to_alter(topo, orderer_id, "OrderingOperator");

  TaskMeta * controller_task = add_operator_to_alter(topo, c_controller_id, "CongestionController");
  add_cfg_to_task(controller_task, "interval", "3000");
  add_edge_to_alter(topo,  c_controller_id, orderer_id);
  add_edge_to_alter(topo,  orderer_id, receiver_id);

  
  for (int i=0; i < CHAINS; ++i) {
    chainHeadIDs[i] = operator_id_t(compID, nextOpID++);
    TaskMeta* task  = add_operator_to_alter(topo, chainHeadIDs[i], "SendK");
    // Send some tuples, e.g. k = 5
    add_cfg_to_task(task, "k",  k_as_str);
    add_cfg_to_task(task, "exit_at_end", "false");
  
    operator_id_t filterID(compID, nextOpID++);
    add_operator_to_alter(topo, filterID, "VariableSamplingOperator");
    
    mockCongestID[i] = operator_id_t(compID, nextOpID++);
    add_operator_to_alter(topo, mockCongestID[i], "MockCongestion");

    add_edge_to_alter(topo,  chainHeadIDs[i], filterID);
    add_edge_to_alter(topo,  filterID, mockCongestID[i]);
    add_edge_to_alter(topo,  mockCongestID[i], c_controller_id);
  }


  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  node.start();
  ControlMessage response;
  node.handle_alter(topo, response);


  shared_ptr<DummyReceiver> dest = boost::dynamic_pointer_cast<DummyReceiver>(
            node.get_operator( receiver_id ));

  shared_ptr<CongestionController> controller = boost::dynamic_pointer_cast<CongestionController>(
            node.get_operator( c_controller_id ));
  
  shared_ptr<MockCongestion> congest_op = boost::dynamic_pointer_cast<MockCongestion>(
            node.get_operator( mockCongestID[0] ));
  
  shared_ptr<SendK> chainHeads[2];
  
  for (int i =0; i < CHAINS; ++i) {
    chainHeads[i] =  boost::dynamic_pointer_cast<SendK>(node.get_operator( chainHeadIDs[i] ));
  }

  int tries = 0;
  while (tries++ < 5 && dest->tuples.size() < CHAINS * K) {
    js_usleep(50 * 1000);
  }

  ASSERT_EQ(CHAINS * K, dest->tuples.size());


  //turn on sampling

  congest_op->congestion = 0.5;
  
  js_usleep(200 * 1000); //wait for update to reach controller
  controller->do_assess(); // should push out update, synchronously.


  cout << "doing restarts" << endl;
  //restart sends
  for (int i =0; i < CHAINS; ++i) {
    chainHeads[i]->reset();
    chainHeads[i]->start();
  }
  
  //should get only half again what we got before, due to backoff
  int expected_after_sampling = 3 * CHAINS * K /2;
  tries = 0;
 
  while (tries++ < 5 && dest->tuples.size() < expected_after_sampling) {
    js_usleep(50 * 1000);
  }

  js_usleep(500 * 1000); //make sure no more arrives
  
    //RNG is allowed to be sloppy so we won't exactly hit target rate
  cout << "got " << dest->tuples.size() << " and expected " << expected_after_sampling << endl;
  ASSERT_LT(expected_after_sampling - 15, dest->tuples.size());
  ASSERT_GT(expected_after_sampling + 15, dest->tuples.size());


  node.stop();
  cout << "done" << endl;
}