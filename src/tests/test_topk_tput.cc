#include <iostream>

#include "js_utils.h"
#include "experiment_operators.h"

//#include "queue_congestion_mon.h"
#include "topk_tput.h"

#include "node.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;


void make_cube(Node& node, std::string& src_cube_name) {

  AlterTopo topo;
  topo.set_computationid(0);

  CubeMeta * cube_meta = add_cube_to_alter(topo, src_cube_name);
  add_dimension(cube_meta, CubeSchema_Dimension_DimensionType_TIME, "time" , 0);
  add_dimension(cube_meta, CubeSchema_Dimension_DimensionType_STRING , "url" , 1);
  add_aggregate(cube_meta, "count" , "count" , 2);
  ControlMessage response;
  node.handle_alter(topo, response);
  EXPECT_FALSE(response.has_error_msg());


  shared_ptr<DataCube> cube = node.get_cube(src_cube_name);
  time_t now = time(NULL);
  for( int i = 0; i < 10; ++i) {
    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple_time(*t, now);
    extend_tuple(*t, "http://foo.com/" + boost::lexical_cast<string>(i));
    extend_tuple(*t, i);
    t->set_version(0);
  }



}

TEST(Topk_TPUT, OneLocal) {

  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  AlterTopo topo;

  operator_id_t dest_id(1,1), coordinator_id(1,2), sender_id(1,3);
  string src_cube_name = "source_cube";

  make_cube(node, src_cube_name); //do this before starting subscribers

  topo.set_computationid(dest_id.computation_id);

  
  add_operator_to_alter(topo, sender_id, "MultiRoundSender");
  add_operator_to_alter(topo, coordinator_id, "MultiRoundCoordinator");
  add_operator_to_alter(topo, dest_id, "DummyReceiver");

  add_edge_to_alter(topo, src_cube_name, sender_id); //should be sender_id
  add_edge_to_alter(topo, sender_id, coordinator_id);
  add_edge_to_alter(topo, coordinator_id, dest_id);
  
  ControlMessage response;
  node.handle_alter(topo, response);
  ASSERT_FALSE(response.has_error_msg());



}