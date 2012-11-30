//
//  two_nodes.h
//  JetStream
//
//  Created by Ariel Rabkin on 11/29/12.
//  Copyright (c) 2012 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_two_nodes_h
#define JetStream_two_nodes_h


#include <boost/asio.hpp>
//#include "simple_net.h"


//namespace jetstream {

class NodeTwoNodesTest : public ::testing::Test {


public:

  virtual void SetUp();
  
  virtual void TearDown();

protected:
  boost::asio::io_service io_service;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sockets[2];
  boost::shared_ptr<SimpleNet> connections[2];
  boost::shared_ptr<Node> nodes[2];

};

//}

#endif
