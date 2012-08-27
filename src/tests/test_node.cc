#include<iostream>
#include <gtest/gtest.h>

#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>
#include <boost/asio.hpp>

#include <netinet/in.h>

#include "node.h"
#include "operators.h"

using namespace std;
using namespace boost;
using namespace boost::asio;
using namespace jetstream;


//helper method to fill in an AlterTopo with a pair of operators
void add_pair_to_topo(AlterTopo& topo)
{
  TaskMeta* task = topo.add_tostart();
  TaskID* id = task->mutable_id();
  id->set_computationid(17);
  id->set_task(2);
  task->set_op_typename("FileRead");
  TaskMeta_DictEntry* op_cfg = task->add_config();
  op_cfg->set_opt_name("file");
  op_cfg->set_val("src/tests/data/base_operators_data.txt");

  task = topo.add_tostart();
  id = task->mutable_id();
  id->set_computationid(17);
  id->set_task(3);
  task->set_op_typename("DummyReceiver");
  
  Edge * e = topo.add_edges();
  e->set_src(2);
  e->set_dest(3);
  e->set_computation(17);
}

TEST(Node, OperatorCreate)
{
  NodeConfig cfg;
  Node node(cfg);
  operator_id_t id(1,2);
  shared_ptr<DataPlaneOperator> op = node.create_operator("test",id);
  ASSERT_TRUE(op != NULL);
  ASSERT_EQ(node.get_operator( id ), op);
}


TEST(Node, HandleAlter_2_Ops)
{
  NodeConfig cfg;
  Node node(cfg);
  AlterTopo topo;

  add_pair_to_topo(topo);
  
  ControlMessage m = node.handle_alter(topo);
  ASSERT_EQ(m.type(), ControlMessage::OK);
  
  operator_id_t id2(17,2);
  shared_ptr<DataPlaneOperator> op = node.get_operator( id2 );
  ASSERT_TRUE(op != NULL);
  
  id2 = operator_id_t(17,3);
  shared_ptr<DataPlaneOperator> dest = node.get_operator( id2 );
  ASSERT_TRUE(dest != NULL);
  
    //TODO better way to wait here?
  boost::this_thread::sleep(boost::posix_time::seconds(1));
  
  DummyReceiver * rec = reinterpret_cast<DummyReceiver*>(dest.get());
  ASSERT_GT(rec->tuples.size(),(unsigned int) 4);
  string s = rec->tuples[0].e(0).s_val();
  ASSERT_TRUE(s.length() > 0 && s.length() < 100); //check that output is a sane string
}


class BindTestThread {
  public:
    NodeConfig& cfg;
    shared_ptr<Node> n;
    BindTestThread(NodeConfig& c): cfg(c) {
      n = shared_ptr<Node>(new Node(cfg));
    }
    void operator()() {
      assert(n);
      n->run();
    }
};


class NodeNetTest : public ::testing::Test {

 public:
  shared_ptr<Node> n;
  asio::io_service io_service;
  ip::tcp::socket cli_socket;

  NodeNetTest():cli_socket(io_service) {
  
  }
  
  
  virtual void SetUp() {
    ip::tcp::acceptor acceptor(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0));
    ip::tcp::endpoint concrete_end = acceptor.local_endpoint();
    
    acceptor.listen();
    pair<string, port_t> p("127.0.0.1", concrete_end.port());
    
    NodeConfig cfg;
    cfg.heartbeat_time = 4000;
    cfg.controllers.push_back(p);

    BindTestThread testThreadBody(cfg);
    thread testThread(testThreadBody);
    // This assignment only works if 'n' is assigned before the constructor returns
    this->n = testThreadBody.n;
    
    boost::system::error_code ec;
    acceptor.accept(cli_socket, ec);
  }
  
    
  virtual void TearDown() {
    if (n) {
      n->stop();
    }
  }
  
  //returns a smart_ptr to a control message
  boost::shared_ptr<ControlMessage> get_ctrl_msg()
  {
    boost::array<char, 4> buf;
    boost::system::error_code error;
    boost::this_thread::sleep(boost::posix_time::seconds(2));
    int len_len = cli_socket.read_some(boost::asio::buffer(buf));

    EXPECT_EQ(len_len, 4);
    int32_t len = ntohl( *(reinterpret_cast<int32_t*> (buf.data())));
    
    std::vector<char> buf2(len);
    int hb_len = cli_socket.read_some(boost::asio::buffer(buf2));
    EXPECT_EQ(len, hb_len); //read completed.
    boost::this_thread::sleep(boost::posix_time::seconds(2));


    boost::shared_ptr<ControlMessage>  h(new ControlMessage);
    h->ParseFromArray(&buf2[0], hb_len);
    return h;
  }
  
    ///test is local, so there's no need for error handling here
  void send_ctrl_msg(google::protobuf::MessageLite& m)
  {
    int sz = m.ByteSize();
    u_int32_t len_nbo = htonl (sz);
    int nbytes = sz + sizeof(int32_t);

    u_int8_t * msg = new u_int8_t[nbytes];

    memcpy(msg, &len_nbo, sizeof(int32_t));
    m.SerializeToArray((msg + sizeof(int32_t)), sz);
    cli_socket.send(asio::buffer(msg, nbytes));
    delete msg;
  }

};

//This test verifies that heartbeats are being sent correctly.
TEST_F(NodeNetTest, NetBind)
{
  ASSERT_TRUE( cli_socket.is_open());

  boost::shared_ptr<ControlMessage> h = get_ctrl_msg();
  ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
  ASSERT_EQ(h->heartbeat().cpuload_pct(), 0);
  
  cout << "test ending" <<endl;
}

//This test verifies that the Node handles requests properly
TEST_F(NodeNetTest, NetStart)
{
  ASSERT_TRUE( cli_socket.is_open());

//create a request

  ControlMessage msg;
  AlterTopo* topo = msg.mutable_alter();
  add_pair_to_topo(*topo);
  msg.set_type(ControlMessage::ALTER);
  //send it
  send_ctrl_msg(msg);
  
  bool found_response = false;
  for (int i =0; i < 3 && !found_response; ++i) {
    
    boost::shared_ptr<ControlMessage> h = get_ctrl_msg();
    
    switch( h->type() ) {
      case ControlMessage::OK:
        cout << "got response back ok from AlterTopo" <<endl;
        found_response = true;
        break;
      case ControlMessage::HEARTBEAT:
        break;
      default:
        cout << "Unexpected message type: " << h->type() <<endl;
        ASSERT_EQ(h->type(), ControlMessage::HEARTBEAT);
        break;
    }
  }

  ASSERT_TRUE(found_response);
  cout << "test ending" <<endl;  

}
