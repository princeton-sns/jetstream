
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

#include "simple_net.h"
#include "two_nodes.h"


const int compID = 4;


TEST(Sampling, TwoLocalChains) {
  const unsigned int CHAINS = 2;
  unsigned int K = 100;
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

  
  for (unsigned int i=0; i < CHAINS; ++i) {
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
  
  for (unsigned int i =0; i < CHAINS; ++i) {
    chainHeads[i] =  boost::dynamic_pointer_cast<SendK>(node.get_operator( chainHeadIDs[i] ));
  }

  unsigned int tries = 0;
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
  for (unsigned int i =0; i < CHAINS; ++i) {
    chainHeads[i]->reset();
    chainHeads[i]->start();
  }
  
  //should get only half again what we got before, due to backoff
  unsigned int expected_after_sampling = 3 * CHAINS * K /2;
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

TEST_F(NodeTwoNodesTest, LocalAndRemoteSampling) {

  const int CHAINS = 2;
  int nextOpID = 1;
  int compID = 1;
  int targetLen = 10;
  int queueWait = 50;
  
  operator_id_t chainHeadIDs[CHAINS], mockCongestID[CHAINS];
  operator_id_t receiver_id(compID, nextOpID++);
  operator_id_t orderer_id(compID, nextOpID++);
  operator_id_t stub_id (compID, nextOpID++);
  operator_id_t c_controller_id(compID, nextOpID++);
  
  {       //First, set up job 1
    AlterTopo topo;
    int i = 0;

    chainHeadIDs[i] = operator_id_t(compID, nextOpID++);
    
    add_operator_to_alter(topo, receiver_id, "DummyReceiver");
    add_operator_to_alter(topo, orderer_id, "OrderingOperator");

    TaskMeta * controller_task = add_operator_to_alter(topo, c_controller_id, "CongestionController");
    add_cfg_to_task(controller_task, "interval", "300");
    add_edge_to_alter(topo,  c_controller_id, orderer_id);
    add_edge_to_alter(topo,  orderer_id, receiver_id);

    TaskMeta* task  = add_operator_to_alter(topo, chainHeadIDs[0], "ContinuousSendK");
    add_cfg_to_task(task, "period",  "20"); //one tuple every 20 ms: 50 /sec
  
    operator_id_t filterID(compID, nextOpID++);
    add_operator_to_alter(topo, filterID, "VariableSamplingOperator");
    
    mockCongestID[i] = operator_id_t(compID, nextOpID++);
    TaskMeta * queueTask = add_operator_to_alter(topo, mockCongestID[i], "FixedRateQueue");
    add_cfg_to_task(queueTask, "ms_wait", boost::lexical_cast<string>(queueWait)); //twenty tuples per second =  20/sec
    add_cfg_to_task(queueTask, "queue_length", boost::lexical_cast<string>(targetLen));

    add_edge_to_alter(topo,  chainHeadIDs[i], filterID);
    add_edge_to_alter(topo,  filterID, mockCongestID[i]);
    add_edge_to_alter(topo,  mockCongestID[i], c_controller_id);


    add_operator_to_alter(topo, stub_id, "OrderingOperator");
    add_edge_to_alter(topo,  stub_id, c_controller_id);

  
    ControlMessage resp;
    nodes[i]->handle_alter(topo,resp);
  }

  shared_ptr<FixedRateQueue> congest_op = boost::dynamic_pointer_cast<FixedRateQueue>(
            nodes[0]->get_operator( mockCongestID[0] ));
  shared_ptr<DummyReceiver> dest = boost::dynamic_pointer_cast<DummyReceiver>(
            nodes[0]->get_operator( receiver_id ));
  
  
  js_usleep(1000 * 1000 * 2);
  int qLen = congest_op->queue_length();
  cout << "queue length is " << qLen << " congestion ratio is " <<
      congest_op->congestion_monitor()->capacity_ratio()  << endl;

  int dest_tuples_halfway = dest->tuples.size();
  cout << "dest received " << dest_tuples_halfway << " tuples" << endl;
  ASSERT_LT(targetLen /2, qLen);
  ASSERT_GT(2 * targetLen, qLen);

  int SECOND_HALF_WAIT = 5; //seconds

  {
    AlterTopo topo;  

    int i = 1;

    chainHeadIDs[i] = operator_id_t(compID, nextOpID++);

    TaskMeta* task  = add_operator_to_alter(topo, chainHeadIDs[i], "ContinuousSendK");
    add_cfg_to_task(task, "period",  "20"); //one tuple every 20 ms: 50 /sec
 
    operator_id_t filterID(compID, nextOpID++);
    add_operator_to_alter(topo, filterID, "VariableSamplingOperator");
    
    add_edge_to_alter(topo,  chainHeadIDs[i], filterID);
    
    Edge * e = add_edge_to_alter(topo,  filterID, stub_id);
    NodeID * destIP = e->mutable_dest_addr();
    const boost::asio::ip::tcp::endpoint& dest_node_addr = nodes[0]->get_listening_endpoint();
    destIP->set_portno(dest_node_addr.port());
    destIP->set_address("127.0.0.1"); 

    ControlMessage resp;
    nodes[i]->handle_alter(topo,resp);
  }
  
  js_usleep(1000 * 1000 * SECOND_HALF_WAIT);
  
  int expected_tuples = SECOND_HALF_WAIT * 1000 * 2 / queueWait + dest_tuples_halfway;
  /**
    The rate-limited source [on node0] will send  1000/queueWait tuples per second.
    The idea of the test is that the non-rate-limited source should be throttled
    down to the same rate. Hence the definition of expected above.
  */
  int dest_tuples_end = dest->tuples.size();
  cout << "dest received " << dest_tuples_end << " tuples" <<
    " and expected "<< expected_tuples << endl;

  ASSERT_LT(expected_tuples * 0.9,  dest_tuples_end);
  ASSERT_GT(expected_tuples * 1.1,  dest_tuples_end);

  cout << "end of test" << endl;
}

