#include "node_dataplane.h"
#include<iostream>

#include <gtest/gtest.h>

using namespace std;
using namespace boost;
using namespace jetstream;

TEST(Node, OperatorCreate)
{
  NodeDataPlaneConfig cfg;
  NodeDataPlane node(cfg);
  operator_id_t id =  {1,2};
  shared_ptr<DataPlaneOperator> op = node.create_operator("test",id);
  ASSERT_TRUE(op != NULL);
  ASSERT_EQ(node.get_operator( id ), op);
}
