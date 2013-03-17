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

static
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

static
void initialize_cube_data(shared_ptr<DataCube> cube, int offset) {
  unsigned int CELLS = 10;

  time_t now = time(NULL);
  for(unsigned int i = 0; i < CELLS; ++i) {;
    boost::shared_ptr<Tuple> t(new Tuple);
    extend_tuple_time(*t, now);
    extend_tuple(*t, "http://foo.com/" + boost::lexical_cast<string>(i));
    extend_tuple(*t,(int) ((i + offset) % CELLS) );
    t->set_version(0);
    cube->process(t);
  }
  int tries = 0;
  while (tries ++ < 20 &&   cube->num_leaf_cells() < CELLS)
    js_usleep(50*1000);
  EXPECT_EQ(CELLS, cube->num_leaf_cells());
}

TEST(Topk_TPUT, TwoLocal) {

  NodeConfig cfg;
  int K = 3;
  boost::system::error_code error;
  Node node(cfg, error);
  node.start();
  ASSERT_TRUE(error == 0);
  AlterTopo topo;

  operator_id_t coordinator_id(1,2); //dest_id(1,1),
  operator_id_t sender_ids[] = {operator_id_t(1,3), operator_id_t(1,4) };
  string src_cube_names[] = {"source1", "source2"};
  string dest_cube_name = "dest_cube";

  for (int i = 0; i < 2; ++i ) {
    shared_ptr<DataCube> src_cube = make_cube(node, src_cube_names[i]); //do this before starting subscribers
    initialize_cube_data(src_cube, i);
    add_edge_to_alter(topo, src_cube_names[i], sender_ids[i]); //should be sender_id
    add_operator_to_alter(topo, sender_ids[i], "MultiRoundSender");
    add_edge_to_alter(topo, sender_ids[i], coordinator_id);
  }
  shared_ptr<DataCube> dest_cube =  make_cube(node, dest_cube_name);
  ASSERT_EQ(0U, dest_cube->num_leaf_cells());

  topo.set_computationid(coordinator_id.computation_id);

  TaskMeta * coord = add_operator_to_alter(topo, coordinator_id, "MultiRoundCoordinator");
  add_cfg_to_task(coord, "num_results", boost::lexical_cast<std::string>(K));
  add_cfg_to_task(coord, "sort_column", "-count");

  add_edge_to_alter(topo, coordinator_id, dest_cube_name);
  
  ControlMessage response;
  node.handle_alter(topo, response);
  ASSERT_FALSE(response.has_error_msg());

  shared_ptr<MultiRoundCoordinator> coord_op =
    boost::dynamic_pointer_cast<MultiRoundCoordinator>(node.get_operator( coordinator_id ));

  //wait for query to run.
  int tries = 0;
  cout << "waiting for tput to run" << endl;
  while (tries ++ < 200 &&  (dest_cube->num_leaf_cells() == 0  || coord_op->phase != NOT_STARTED))
    js_usleep(50*1000);

  cout << "Done running, waiting for commits";
  dest_cube->wait_for_commits();
  js_usleep(500*1000);  // and wait for DB to process the update

  
  //now check that DB has what it should
  list<string> dims;
  dims.push_back("-count");
  cube::CubeIterator it = dest_cube->slice_query(dest_cube->empty_tuple(), dest_cube->empty_tuple(), true, dims, K);
  cout << "output: " <<endl;
  int expected = 17;
  while ( it != dest_cube->end()) {
    shared_ptr<Tuple> t = *it;
    ASSERT_EQ(expected, t->e(2).i_val());
    expected -= 2;
    cout << fmt(*t) << endl;
    it++;
  }
  ASSERT_TRUE( expected < 17);
  cout << "done" << endl;
  node.stop();
}
