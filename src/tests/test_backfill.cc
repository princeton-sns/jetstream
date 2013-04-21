#include <gtest/gtest.h>
#include <glog/logging.h>

#include "js_utils.h"

#include "experiment_operators.h"
#include "base_operators.h"
#include "node.h"

using namespace ::std;
using namespace boost;
using namespace jetstream;


TEST(Backfill, OperatorChain) {

  ExtendOperator ex_1;
  shared_ptr<DummyReceiver> rec(new DummyReceiver);

  operator_config_t cfg;
  cfg["types"] = "i";
  cfg["0"] = "1";
  ex_1.configure(cfg);
  ex_1.set_dest(rec);

  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple(*t, 2);
  ex_1.process(t);

  ASSERT_EQ((size_t)1, rec->tuples.size());
  ASSERT_EQ(2, rec->tuples[0]->e_size()); //buffer holds (2,1)

  t->clear_e();
  extend_tuple(*t, 2);

  boost::shared_ptr<Tuple> t2(new Tuple);
  extend_tuple(*t2, 5);
  operator_id_t no_id;
  ex_1.process_delta(*t, t2, no_id);

  ASSERT_EQ((size_t)1, rec->tuples.size());
  ASSERT_EQ(2, rec->tuples[0]->e_size());
  ASSERT_EQ(5, rec->tuples[0]->e(0).i_val());
}


static shared_ptr<DataCube>
make_cube(Node& node, std::string& src_cube_name) {

  AlterTopo topo;
  topo.set_computationid(0);

  CubeMeta * cube_meta = add_cube_to_alter(topo, src_cube_name);
  add_dimension(cube_meta, CubeSchema_Dimension_DimensionType_TIME, "time" , 0);
//  add_dimension(cube_meta, CubeSchema_Dimension_DimensionType_STRING , "url" , 1);
 // jetstream::CubeSchema_Aggregate * a =
  add_aggregate(cube_meta, "avg" , "myavg" , 2);
  add_aggregate(cube_meta, "min_i" , "mymin" , 4);
  add_aggregate(cube_meta, "count" , "mycount" , 5);


  ControlMessage response;
  node.handle_alter(topo, response);
  EXPECT_FALSE(response.has_error_msg());

  shared_ptr<DataCube> cube = node.get_cube(src_cube_name);

  return cube;
}

static
boost::shared_ptr<Tuple> get_tuple(shared_ptr<DataCube> cube) {
  for(int i =0; i < 20 &&  cube->num_leaf_cells() < 1; i++) {
    js_usleep(100 * 1000);
  }

  Tuple empty = cube->empty_tuple();
  EXPECT_EQ(1U, cube->num_leaf_cells());
  cube::CubeIterator it = cube->slice_query(empty, empty);
  return *it;
}

TEST(Backfill, IntoCube) {

  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  node.start();
  ASSERT_TRUE(error == 0);
  string cubename("test_cube");
  shared_ptr<DataCube> cube = make_cube(node, cubename);

  time_t now = time(NULL);
  boost::shared_ptr<Tuple> t(new Tuple);
  extend_tuple_time(*t, now);
  t->add_e();
  extend_tuple(*t, 10);
  extend_tuple(*t, 1);
  extend_tuple(*t, 6);
  cube->process(t);
  
    //cube should have one tuple:  (now, 10, 1, 6 1)
  boost::shared_ptr<Tuple> t_out = get_tuple(cube);
  cout << "got tuple " << fmt(*t_out) << endl;
  ASSERT_EQ(4, t_out->e_size());
  ASSERT_EQ(10, t_out->e(1).i_val());


  boost::shared_ptr<Tuple> t2(new Tuple);
  extend_tuple_time(*t2, now);
  t2->add_e();
  extend_tuple(*t2, 15);
  extend_tuple(*t2, 3);
  extend_tuple(*t2, 4);
  operator_id_t no_oper_id(0,0);
  cube->process_delta(*t, t2);

  cube->wait_for_commits();
  t_out = get_tuple(cube);
  cout << "got tuple " << fmt(*t_out) << endl;
  ASSERT_EQ(1U, cube->num_leaf_cells());
  ASSERT_EQ(5, t_out->e(1).i_val());


  node.stop();
}

