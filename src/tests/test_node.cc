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


class bind_test_thread {
  public:
    NodeConfig& cfg;
    Node * n;
    bind_test_thread(NodeConfig& c): cfg(c),n(NULL) {}
    ~bind_test_thread() {
        if (n != NULL) {
          n->stop();
          delete n;
        }
    }
    void operator()();    
};


void
bind_test_thread::operator()()
{
  n = new Node(cfg);
  n->run();
}

TEST(Node, NetBind)
{
  
  io_service io_service;
  ip::tcp::acceptor acceptor(io_service, ip::tcp::endpoint(ip::tcp::v4(), 0));
  ip::tcp::endpoint concrete_end = acceptor.local_endpoint();
//  cout << "endpoint was " << concrete_end.address()<<":" << concrete_end.port() <<endl;
  
  acceptor.listen();
  pair<string, port_t> p("127.0.0.1", concrete_end.port());

  
  NodeConfig cfg;
  cfg.heartbeat_time = 1000;
  cfg.controllers.push_back(p);

  bind_test_thread test_thread_body(cfg);
  thread test_thread(test_thread_body);
  
//  Node node(cfg);
  
  ip::tcp::socket cli_socket(io_service);
  boost::system::error_code ec;
  
  acceptor.accept(cli_socket, ec);
  
  ASSERT_TRUE( cli_socket.is_open());

  boost::array<char, 4> buf;
  boost::system::error_code error;
  boost::this_thread::sleep(boost::posix_time::seconds(0));
  int len_len = cli_socket.read_some(boost::asio::buffer(buf));

  ASSERT_EQ(len_len, 4);
  int32_t len = ntohl( *(reinterpret_cast<int32_t*> (buf.data())));
  
  std::vector<char> buf2(len);
  int hb_len = cli_socket.read_some(boost::asio::buffer(buf2));
  ASSERT_EQ(len, hb_len); //read completed.
  boost::this_thread::sleep(boost::posix_time::seconds(2));


  ControlMessage h;
  h.ParseFromArray(buf2.data(), hb_len);
  ASSERT_EQ(h.type(), ControlMessage::HEARTBEAT);
  
  ASSERT_EQ(h.heartbeat().cpuload_pct(), 0);
  
  cout << "test ending" <<endl;
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
  
  node.handle_alter(topo);
  
  
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
