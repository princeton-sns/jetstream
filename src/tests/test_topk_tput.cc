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


shared_ptr<DataCube> make_cube(Node& node, std::string& src_cube_name) {

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

  return cube;
}

void initialize_cube_data(shared_ptr<DataCube> cube) {
  int CELLS = 10;

  time_t now = time(NULL);
  for( int i = 0; i < CELLS; ++i) {
    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple_time(*t, now);
    extend_tuple(*t, "http://foo.com/" + boost::lexical_cast<string>(i));
    extend_tuple(*t, i);
    t->set_version(0);
    cube->process(t);
  }
  int tries = 0;
  while (tries ++ < 20 &&   cube->num_leaf_cells() < CELLS)
    js_usleep(50*1000);
  EXPECT_EQ(CELLS, cube->num_leaf_cells());
}

TEST(Topk_TPUT, OneLocal) {

  NodeConfig cfg;
  int K = 5;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);
  AlterTopo topo;

  operator_id_t coordinator_id(1,2), sender_id(1,3); //dest_id(1,1), 
  string src_cube_name = "source_cube";
  string dest_cube_name = "dest_cube";

  shared_ptr<DataCube> src_cube = make_cube(node, src_cube_name); //do this before starting subscribers
  initialize_cube_data(src_cube);
  shared_ptr<DataCube> dest_cube =  make_cube(node, dest_cube_name);

  topo.set_computationid(coordinator_id.computation_id);

  add_operator_to_alter(topo, sender_id, "MultiRoundSender");
  TaskMeta * coord = add_operator_to_alter(topo, coordinator_id, "MultiRoundCoordinator");
  add_cfg_to_task(coord, "num_results", boost::lexical_cast<std::string>(K));
  add_cfg_to_task(coord, "sort_column", "count");

//  add_operator_to_alter(topo, dest_id, "DummyReceiver");

  add_edge_to_alter(topo, src_cube_name, sender_id); //should be sender_id
  add_edge_to_alter(topo, sender_id, coordinator_id);
//  add_edge_to_alter(topo, coordinator_id, dest_id);
  add_edge_to_alter(topo, coordinator_id, dest_cube_name);
  
  ControlMessage response;
  node.handle_alter(topo, response);
  ASSERT_FALSE(response.has_error_msg());


  //wait for query to run.

  int tries = 0;
  while (tries ++ < 20 &&  dest_cube->num_leaf_cells() < K)
    js_usleep(50*1000);

  
  //now check that DB has what it should
  cube::CubeIterator it = dest_cube->slice_query(dest_cube->empty_tuple(), dest_cube->empty_tuple(), true);
  cout << "output: " <<endl;
  while ( it != dest_cube->end()) {
    shared_ptr<Tuple> t = *it;
    cout << fmt(*t) << endl;
    it++;
  }
  cout << "done" << endl;

}