/**
  This file has node "unit" tests that don't require big topologies as inputs
  and that don't use test fixtures.
*/


#include <iostream>
#include <gtest/gtest.h>

#include <boost/date_time.hpp>
#include <boost/asio.hpp>

#include "node.h"
#include "base_operators.h"
#include "simple_net.h"
#include "experiment_operators.h"

using namespace std;
using namespace boost;
using namespace boost::asio;
using namespace boost::asio::ip;
using namespace jetstream;




TEST(Node,Ctor) {
  NodeConfig cfg;
  boost::system::error_code err;
  Node n(cfg,err);
}

TEST(Node,BareStop) {
  NodeConfig cfg;
  boost::system::error_code err;
  Node n(cfg,err);
  n.stop();
  n.stop();
}

TEST(Node, OperatorCreateDestroy)
{
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);

  operator_id_t id(1,2);
  operator_config_t oper_cfg;
  shared_ptr<COperator> op = node.create_operator("test",id, oper_cfg);
  ASSERT_TRUE(op != NULL);
}

TEST(Node, UnregisterNonOperator)
{
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  operator_id_t id(1,2);
  node.unregister_operator(id);
  node.unregister_operator(id);//do it again
}

TEST(Node, BadOperatorName) {
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  AlterTopo topo;
  
  TaskMeta* task = topo.add_tostart();
  TaskID * id = task->mutable_id();
  id->set_computationid( 1 );
  id->set_task(1);
  task->set_op_typename("SendK");
  
  task = topo.add_tostart();
  id = task->mutable_id();
  id->set_computationid( 1 );
  id->set_task(2);
  task->set_op_typename("no such name");
  
  task = topo.add_tostart();
  id = task->mutable_id();
  id->set_computationid( 1 );
  id->set_task(3);
  task->set_op_typename("SendK");
  
  ControlMessage r;
  node.handle_alter(topo, r);
  ASSERT_EQ(r.type(), ControlMessage::ERROR);
  ASSERT_EQ(0U, node.operator_count());  //all or none should start
}


TEST(Node, DuplicateOperator) {
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  AlterTopo topo;

  operator_id_t id (1,1);
  add_operator_to_alter(topo, id, "SendK");
  add_operator_to_alter(topo, id, "SendK");


  ControlMessage r;
  node.handle_alter(topo, r);
  ASSERT_EQ(r.type(), ControlMessage::ERROR);
  ASSERT_EQ(node.operator_count(), 0U);  //all or none should start

}


TEST(Node, BadOperatorConfig) {
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  AlterTopo topo;
  
  TaskMeta* task = topo.add_tostart();
  TaskID * id = task->mutable_id();
  id->set_computationid( 1 );
  id->set_task(1);
  task->set_op_typename("SendK");
  TaskMeta_DictEntry* op_cfg = task->add_config();
  op_cfg->set_opt_name("k");
  op_cfg->set_val("nanana");
  
  
  ControlMessage r;
  node.handle_alter(topo, r);
  ASSERT_EQ(r.type(), ControlMessage::ERROR);
  ASSERT_EQ(node.operator_count(), 0U);
}


//verify that web interface starts and stops are properly idempotent/repeatable.
TEST(Node,WebIfaceStartStop)
{
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);

  NodeWebInterface iface("0.0.0.0", (port_t) 8081, node);
  
  for(int i=0; i < 10; ++i) {
    iface.start();
    iface.stop();
    iface.stop();
  }
}

TEST(Node,DuplicateStart) {
  NodeConfig cfg;
  boost::system::error_code err;
//  cfg.webinterface_port = 8082;

  Node n(cfg,err);
  n.start();

  tcp::endpoint e = n.get_listening_endpoint();
  cfg.dataplane_ep.second = e.port();
  Node n2(cfg, err);
  n2.start();

  cout << "duplicate start ok" << endl;
  n.stop();
  n2.stop();
}
