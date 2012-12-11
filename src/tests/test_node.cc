#include<iostream>
#include <gtest/gtest.h>

//#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>
#include <boost/asio.hpp>

#include "js_utils.h"
#include "node.h"
#include "base_operators.h"
#include "simple_net.h"
#include "experiment_operators.h"


using namespace std;
using namespace boost;
using namespace boost::asio;
using namespace boost::asio::ip;
using namespace jetstream;

#include "two_nodes.h"


int COMP_ID = 17;

//helper method to fill in an AlterTopo with a pair of operators
void add_pair_to_topo(AlterTopo& topo, int compID) {
  topo.set_computationid(compID);
  TaskMeta* task = topo.add_tostart();
  TaskID* id = task->mutable_id();
  id->set_computationid(compID);
  id->set_task(2);
  task->set_op_typename("FileRead");
  TaskMeta_DictEntry* op_cfg = task->add_config();
  op_cfg->set_opt_name("file");
  op_cfg->set_val("src/tests/data/base_operators_data.txt");

  task = topo.add_tostart();
  id = task->mutable_id();
  id->set_computationid(compID);
  id->set_task(3);
  task->set_op_typename("DummyReceiver");
  
  Edge * e = topo.add_edges();
  e->set_src(2);
  e->set_dest(3);
  e->set_computation(compID);
}


TEST(Node, HandleAlter_2_Ops)
{
  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);

  node.start();
  for (int i =0; i < 4; ++i) {
    AlterTopo topo;
    add_pair_to_topo(topo, i);
    
    ControlMessage r;
    node.handle_alter(topo, r);
    ASSERT_EQ(r.type(), ControlMessage::ALTER_RESPONSE);
    
    operator_id_t id2(i,2);
    shared_ptr<DataPlaneOperator> op = node.get_operator( id2 );
    ASSERT_TRUE(op != NULL);
    
    id2 = operator_id_t(i,3);
    shared_ptr<DataPlaneOperator> dest = node.get_operator( id2 );
    ASSERT_TRUE(dest != NULL);
    
    DummyReceiver * rec = reinterpret_cast<DummyReceiver*>(dest.get());
    int tries = 0;
    while (rec->tuples.size() < 5 && tries++ < 5)
      boost::this_thread::sleep(boost::posix_time::seconds(1));

    
    ASSERT_GT(rec->tuples.size(), (unsigned int) 4);
    string s = rec->tuples[0]->e(0).s_val();
    ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
    node.stop_computation(i);
  }
}


class NodeNetTest : public ::testing::Test {

 public:
  shared_ptr<Node> n;
  // Order matters here! The constructor initializes io_service before the socket
  // (also see superfluous initializer below)
  asio::io_service io_service;
  ip::tcp::socket cli_socket;
  SimpleNet synch_net;

  // Include a superfluous io_service initializer to cause a compile error if the
  // order of initialization above is switched
  NodeNetTest() : io_service(), cli_socket(io_service), synch_net(cli_socket) {}

  
  virtual void SetUp() {
    ip::tcp::acceptor acceptor(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0));
    ip::tcp::endpoint concrete_end = acceptor.local_endpoint();
    
    acceptor.listen();
    pair<string, port_t> p("127.0.0.1", concrete_end.port());
    
    NodeConfig cfg;
    cfg.heartbeat_time = 2000;
    cfg.controllers.push_back(p);
  
    boost::system::error_code error;
    n = shared_ptr<Node>(new Node(cfg, error));
    EXPECT_TRUE(error == 0);
    n->start();
    
    boost::system::error_code ec;
    acceptor.accept(cli_socket, ec);
  }
  
    
  virtual void TearDown() {
    boost::system::error_code err;
    cli_socket.shutdown(tcp::socket::shutdown_both, err);
    cli_socket.close();
    assert(n);
    cout << "stopping node" << endl;
    n->stop();
  }

};


//This test verifies that heartbeats are being sent correctly.
TEST_F(NodeNetTest, NetBind)
{
  ASSERT_TRUE( cli_socket.is_open());

  boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();
  ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
  ASSERT_EQ(h->heartbeat().cpuload_pct(), 0);
  cout << "leaving NetBindTest; starting teardown" << endl;
}




//This test verifies that heartbeats are being sent correctly.
TEST_F(NodeNetTest, ControllerDrop)
{
  ASSERT_TRUE( cli_socket.is_open());

  boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();
  ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
  ASSERT_EQ(h->heartbeat().cpuload_pct(), 0);
  cout << "stopping network; node should not crash" << endl;
  synch_net.close();
  
}




//This test verifies that the Node handles requests properly
TEST_F(NodeNetTest, NetStartStop)
{
  ASSERT_TRUE( cli_socket.is_open());

//create a request

  ControlMessage msg;
  AlterTopo* topo = msg.mutable_alter();
  add_pair_to_topo(*topo, COMP_ID);
  msg.set_type(ControlMessage::ALTER);
  //send it
  synch_net.send_msg(msg);
  
  bool found_response = false;
  for (int i =0; i < 3 && !found_response; ++i) {
    boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();
    
    switch( h->type() ) {
      case ControlMessage::ALTER_RESPONSE:
        cout << "got response back ok from AlterTopo" <<endl;
        ASSERT_EQ(h->alter().tostart_size(), 2);
        
        found_response = true;
        break;
      case ControlMessage::HEARTBEAT:
        break;
      default:
        cout << "Unexpected message type: " << h->type() <<endl;
        ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT); //will always fail if reached. This is deliberate.
        break;
    }
  }

  ASSERT_TRUE(found_response);
  ASSERT_GT(n->operator_count(), 0); //might be 1 or 2, depending when file reader stops
  
  msg.Clear();
  msg.set_type(ControlMessage::STOP_COMPUTATION);
  msg.set_comp_to_stop(COMP_ID);
  synch_net.send_msg(msg);
  
  cout << "sent stop" <<endl;

  found_response = false;
  for (int i =0; i < 3 && !found_response; ++i) {
    boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();    
    switch( h->type() ) {
      case ControlMessage::ALTER_RESPONSE:
        cout << "got response back from stop" <<endl;
        ASSERT_GT(h->alter().tasktostop_size(), 0);
        found_response = true;
        break;
      case ControlMessage::HEARTBEAT:
        break;
      default:
        cout << "Unexpected message type: " << h->type() <<endl;
        ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT); //will always fail if reached. This is deliberate.
        break;
    }
  }  
  
  ASSERT_EQ(0, n->operator_count());
  
  
}

shared_ptr<DataPlaneOperator> 
add_operator_to_node(Node& n, operator_id_t dest_id, const string& name)
{
  AlterTopo topo;
  ControlMessage r;
  topo.set_computationid(dest_id.computation_id);

  add_operator_to_alter(topo, dest_id, name);
  n.handle_alter(topo, r);
  shared_ptr<DataPlaneOperator> dest = n.get_operator( dest_id );
  EXPECT_TRUE( dest != NULL );
  return dest;
}

shared_ptr<DataPlaneOperator> 
add_dummy_receiver(Node& n, operator_id_t dest_id) {
  return  add_operator_to_node(n, dest_id, "DummyReceiver");
}


// This test verifies that the dataplane can receive data if the operator is ready.
TEST_F(NodeNetTest, ReceiveDataReady)
{
  ASSERT_TRUE( cli_socket.is_open());

  boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();
  ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
  ASSERT_EQ(h->heartbeat().cpuload_pct(), 0);

  operator_id_t dest_id(17,3);
  shared_ptr<DataPlaneOperator> dest = add_dummy_receiver(*n, dest_id);

  //At this point we have a receiver ready to go. Next: connect to it
  
  boost::asio::io_service iosrv;
  tcp::socket socket(iosrv);
  boost::system::error_code cli_error;
  socket.connect(n->get_listening_endpoint(), cli_error);
  SimpleNet data_conn(socket);
  
  cout <<"connected to data port: " << n->get_listening_endpoint().port() << endl;
  
  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  Edge * edge = data_msg.mutable_chain_link();
  edge->set_computation(17);
  edge->set_dest(3);
  edge->set_src(5);
  data_conn.send_msg(data_msg);
  
  cout <<"sent chain connect; data length = " << data_msg.ByteSize() << endl;

  
  shared_ptr<DataplaneMessage> resp = data_conn.get_data_msg();
  ASSERT_EQ(DataplaneMessage::CHAIN_READY, resp->type());
  
  cout <<"got chain ready" << endl;

  data_msg.Clear();
  data_msg.set_type(DataplaneMessage::DATA);
  Tuple * t = data_msg.add_data();
  Element * e = t->add_e();
  e->set_s_val("some mock data");

  data_conn.send_msg(data_msg);
  
  cout <<"sent mock data; data length = " << data_msg.ByteSize() << endl;

  //TODO better way to wait here?
  
  DummyReceiver * rec = reinterpret_cast<DummyReceiver*>(dest.get());
  int tries = 0;
  while (rec->tuples.size() == 0 && tries++ < 20)
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));

  
  ASSERT_EQ(rec->tuples.size(), (unsigned int) 1);
  
  data_msg.Clear();
  data_msg.set_type(DataplaneMessage::NO_MORE_DATA);
  data_conn.send_msg(data_msg);
  
  tries = 0;
  while (data_conn.is_connected() && tries++ < 5) {
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    data_conn.send_msg(data_msg);
  }
  
  ASSERT_FALSE( data_conn.is_connected());
  
  
}


// This test verifies that the dataplane can receive data if the operator
// is not yet ready.
TEST_F(NodeNetTest, ReceiveDataNotYetReady)
{
  ASSERT_TRUE( cli_socket.is_open());

  boost::shared_ptr<ControlMessage> h = synch_net.get_ctrl_msg();
  ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
  ASSERT_EQ(h->heartbeat().cpuload_pct(), 0);

  boost::asio::io_service iosrv;
  tcp::socket socket(iosrv);
  boost::system::error_code cli_error;
  socket.connect(n->get_listening_endpoint(), cli_error);
  SimpleNet data_conn(socket);
  
  cout <<"connected to data port: " << n->get_listening_endpoint().port() << endl;
  
  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  Edge * edge = data_msg.mutable_chain_link();
  edge->set_computation(17);
  edge->set_dest(3);
  edge->set_src(5);
  data_conn.send_msg(data_msg);
  
  cout <<"sent chain connect; data length = " << data_msg.ByteSize() << endl;

  // Create the receiver
  operator_id_t dest_id(17,3);

  shared_ptr<DataPlaneOperator> dest = add_dummy_receiver(*n, dest_id);

  
  shared_ptr<DataplaneMessage> resp = data_conn.get_data_msg();
  ASSERT_EQ(DataplaneMessage::CHAIN_READY, resp->type());
  
  cout <<"got chain ready" << endl;

  data_msg.Clear();
  data_msg.set_type(DataplaneMessage::DATA);
  Tuple * t = data_msg.add_data();
  Element * e = t->add_e();
  e->set_s_val("some mock data");

  data_conn.send_msg(data_msg);
  
  cout <<"sent mock data; data length = " << data_msg.ByteSize() << endl;
  DummyReceiver * rec = reinterpret_cast<DummyReceiver*>(dest.get());

  //TODO better way to wait here?
  int tries = 0;
  while (rec->tuples.size() == 0 && tries++ < 20)
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
  
  ASSERT_EQ(rec->tuples.size(), (unsigned int) 1);
  
  DataplaneMessage echo;
  echo.set_type(DataplaneMessage::TS_ECHO);
  echo.set_timestamp(1);
  data_conn.send_msg(echo);
  cout << "sent ping" << endl;
  resp = data_conn.get_data_msg();
  cout << "got response" << endl;
  ASSERT_EQ(echo.Utf8DebugString(), resp->Utf8DebugString());
  
}



void
NodeTwoNodesTest::SetUp() {
  ip::tcp::acceptor acceptor(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0));
  ip::tcp::endpoint concrete_end = acceptor.local_endpoint();
  acceptor.listen();
  pair<string, port_t> p("127.0.0.1", concrete_end.port());


  NodeConfig cfg;
  cfg.heartbeat_time = 2000;
  cfg.controllers.push_back(p);
  
  boost::system::error_code err;

  for (int i = 0; i < 2; ++i) {

    boost::system::error_code error;
    nodes[i] = shared_ptr<Node>(new Node(cfg, error));
    EXPECT_TRUE(error == 0);
    nodes[i]->start();
  
    sockets[i] = shared_ptr<tcp::socket>(new tcp::socket(io_service));
    acceptor.accept(*sockets[i], err);
    connections[i] = shared_ptr<SimpleNet>(new SimpleNet(*sockets[i]));
    connections[i]->get_ctrl_msg();
  }
  cout << "created nodes, got heartbeats" << endl;
}

void
NodeTwoNodesTest::TearDown() {
  boost::system::error_code err;

  cout << "done with test; tearing down" << endl;
  // Close 'control' sockets first to avoid badness related to io_service destruction
  for (int i =0; i < 2; ++i) {
    sockets[i]->shutdown(tcp::socket::shutdown_both, err);
    sockets[i]->close();
  }

//  cout << "control sockets closed" << endl;
//  js_usleep(500 * 1000);

  for (int i =0; i < 2; ++i) {
    nodes[i]->stop();
//    cout << "stopped node " << i << endl;
    js_usleep(500 * 1000);
    
  }
  cout << "nodes stopped, leaving TearDown" << endl;
}


Edge * 
add_edge_to_alter(AlterTopo& topo, operator_id_t src_id, operator_id_t dest_id, const Node& dest_node) {
  Edge * e = add_edge_to_alter(topo,  src_id, dest_id);
  NodeID * destIP = e->mutable_dest_addr();
  const boost::asio::ip::tcp::endpoint& dest_node_addr = dest_node.get_listening_endpoint();
  destIP->set_portno(dest_node_addr.port());
  destIP->set_address("127.0.0.1");
  return e;
}

// This test is similar to ReceiveDataNotYetReady but uses real dataplane nodes.
// Thus it can test the following scenario: an operator on one node attempts to
// send data to a remote operator on the second node before receiving CHAIN_READY.
// The RemoteDestAdaptor on the first node should block until the chains is ready.
TEST_F(NodeTwoNodesTest, DataplaneConn) {


  operator_id_t dest_id(17,3), src_id(17,2);

  // Start an operator on one node before even creating the receiver; it will 
  // try to send data but will block until the receiver is ready
  string kStr = "5";
  AlterTopo topo;
  topo.set_computationid(src_id.computation_id);

  TaskMeta* task =  add_operator_to_alter(topo, src_id, "SendK");
  // Send some tuples, e.g. k = 5
  add_cfg_to_task(task, "k", kStr);
  add_edge_to_alter(topo, src_id, dest_id, *nodes[0]);

  ControlMessage r;
  nodes[1]->handle_alter(topo, r);  //starting on node 0, ordering it to send to node 1
  
  int tries = 0;
  while (nodes[1]->operator_count() == 0 && tries++ < 20) //wait for alter to be processed
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));

  // Create the receiver 
  shared_ptr<DataPlaneOperator> dest = add_dummy_receiver(*nodes[0], dest_id);
  cout << "created receiver" << endl;

  // Wait for the chain to be ready and the sending operator's data to flow through.   
  DummyReceiver * rec = reinterpret_cast<DummyReceiver*>(dest.get());
  
  u_int k;
  stringstream(kStr) >> k;
  
  tries = 0;
  while (rec->tuples.size() < k && tries++ < 5)
    boost::this_thread::sleep(boost::posix_time::seconds(1));
  
  if(tries >= 20) {
    cout << "GAVE UP WAITING FOR DATA"<<endl;
  }
  
  // EXPECT_* records the failure but allows the cleanup code below to execute
  EXPECT_EQ(k, rec->tuples.size());

}

TEST_F(NodeTwoNodesTest, RemoteCongestionSignal) {

  //create dest and congestion monitor

  operator_id_t dest_id(1,1), congest_op_id(1,2), src_op_id(1,3);

  ControlMessage response;

    //Set up destination
  {
    AlterTopo dest_topo;
    dest_topo.set_computationid(dest_id.computation_id);

    add_operator_to_alter(dest_topo, dest_id, "DummyReceiver");
    add_operator_to_alter(dest_topo, congest_op_id, "MockCongestion");
    add_edge_to_alter(dest_topo, congest_op_id, dest_id);
    nodes[0]->handle_alter(dest_topo, response);
    ASSERT_FALSE(response.has_error_msg());
  }

  EXPECT_EQ(2, nodes[0]->operator_count());
    //TODO check for OK here
  
  shared_ptr<DummyReceiver> dest = boost::dynamic_pointer_cast<DummyReceiver>(
            nodes[0]->get_operator( dest_id ));
  shared_ptr<MockCongestion> congest_op = boost::dynamic_pointer_cast<MockCongestion>(
            nodes[0]->get_operator( congest_op_id ));
  
  congest_op->congestion = 0;
  
  
  // Sources
  {
    AlterTopo send_topo;
    send_topo.set_computationid(src_op_id.computation_id);
    TaskMeta* task  = add_operator_to_alter(send_topo, src_op_id, "SendK");
    // Send some tuples, e.g. k = 5
    add_cfg_to_task(task, "k", "5");
    add_cfg_to_task(task, "exit_at_end", "false");
    add_edge_to_alter(send_topo,  src_op_id, congest_op_id, *nodes[0]);
    
    nodes[1]->handle_alter(send_topo, response);
    ASSERT_FALSE(response.has_error_msg());
  }
  shared_ptr<SendK> src_op =  boost::dynamic_pointer_cast<SendK>(nodes[1]->get_operator( src_op_id ));
  
  
  size_t k = 5;

  size_t tries = 0;
  while (dest->tuples.size() < k && tries++ < 5)
    js_usleep(100 * 1000);

  js_usleep(100 * 1000); //time for signal to propagate


  //TODO arrange for more sends
  
  src_op->reset();
  src_op->start();

  tries = 0;    //no more sends
  while (tries++ < 10) {  //wait a second, make sure we're still blocking the source
    ASSERT_EQ(k, dest->tuples.size());
    js_usleep(100 * 1000);
  }
  
  congest_op->congestion = 10; //congestion resolved!

  tries = 0;
  while (tries++ < 5 && dest->tuples.size() < 2 * k) {
    js_usleep(200 * 1000);
  }

  ASSERT_EQ(2 * k, dest->tuples.size());
}


TEST_F(NodeTwoNodesTest, SuddenStop)  {

  operator_id_t dest_id(1,1), src_op_id(1,2);

  ControlMessage response;

    //Set up destination
  {
    AlterTopo dest_topo;
    dest_topo.set_computationid(dest_id.computation_id);

    add_operator_to_alter(dest_topo, dest_id, "DummyReceiver");
    nodes[0]->handle_alter(dest_topo, response);
    ASSERT_FALSE(response.has_error_msg());
  }
  
  shared_ptr<DummyReceiver> dest = boost::dynamic_pointer_cast<DummyReceiver>(
            nodes[0]->get_operator( dest_id ));

  {
    AlterTopo send_topo;
    send_topo.set_computationid(src_op_id.computation_id);
    TaskMeta* task  = add_operator_to_alter(send_topo, src_op_id, "ContinuousSendK");
    add_cfg_to_task(task, "period",  "20"); //one tuple every 20 ms: 50 /sec
    add_edge_to_alter(send_topo,  src_op_id, dest_id, *nodes[0]);

    nodes[1]->handle_alter(send_topo, response);
    ASSERT_FALSE(response.has_error_msg());
  }

  js_usleep(100 * 1000);  //make sure we're running.

  int tuple_count = dest->tuples.size();
  ASSERT_LT(1, tuple_count);
  cout << "-----------doing stop after " << tuple_count << " tuples received -----------" << endl;
  //FIXME: do we need to close ctrl connection here?
  nodes[0]->stop();
  js_usleep(200 * 1000);  //wait and make sure nothing pops on sender side
  cout << "-----------cleaning up source-----------" << endl;
  
  //TODO check that send operator is stopped?


}
