
#include <iostream>

#include "js_utils.h"
#include "experiment_operators.h"

#include "queue_congestion_mon.h"
#include "node.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;

const int compID = 4;


TEST(Sampling, TwoLocalChains) {
  int CHAINS = 2;
  shared_ptr<SendK> chainHeads[2];
  operator_id_t chainHeadIDs[2];
  
  int nextOpID = 1;
  int compID = 1;
  
  NodeConfig cfg;
  boost::system::error_code error;
  
  AlterTopo topo;  
  
  for (int i=0; i < CHAINS; ++i) {
  
  
  }

  Node node(cfg, error);
  ASSERT_TRUE(error == 0);

  node.start();
  ControlMessage response;
  
  node.handle_alter(topo, response);



  for (int i =0; i < CHAINS; ++i) 
    chainHeads[i] =  boost::dynamic_pointer_cast<SendK>(node.get_operator( chainHeadIDs[i] ));


}