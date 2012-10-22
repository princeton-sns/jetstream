

#include <iostream>
#include "cube_manager.h"
#include "node.h"

#include "base_subscribers.h"
#include "base_operators.h"
#include "js_utils.h"


#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using namespace ::std;




const char * TEST_CUBE = "test_cube";

class SubscriberTest : public ::testing::Test {
  protected:
  
  Node * node;

  virtual void SetUp() {

    int compID = 4;

    NodeConfig cfg;
    boost::system::error_code error;
    node = new Node(cfg, error);
    ASSERT_TRUE(error == 0);

    AlterTopo topo;
    topo.set_computationid(compID);

    jetstream::CubeMeta * cube_meta = topo.add_tocreate();
    cube_meta->set_name("text");

    jetstream::CubeSchema * sc = cube_meta->mutable_schema();
    cube_meta->set_name(TEST_CUBE);
    cube_meta->set_overwrite_old(true);

    jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
    dim->set_type(Element_ElementType_STRING);
    dim->set_name("url");
    dim->add_tuple_indexes(0);

    dim = sc->add_dimensions();
    dim->set_type(Element_ElementType_TIME);
    dim->set_name("time");
    dim->add_tuple_indexes(1);

    jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
    agg->set_name("count");
    agg->set_type("count");
    agg->add_tuple_indexes(2);
    

//  cout << topo.Utf8DebugString();

    ControlMessage r;
    node->handle_alter(r, topo);
    cout << "alter sent; cube should be present" << endl;
    
  }
  
  void add_tuples(shared_ptr<DataCube> cube) {
    for (int i =0; i < 1; ++i) {
      boost::shared_ptr<Tuple> t(new Tuple);

      extend_tuple(*t, "http://foo.com");
      extend_tuple_time(*t, i);
      extend_tuple(*t, i+1);

      cout<< "Tuple:" << fmt(*t) << endl;
      cube->process(t);
    }
  
  }
  
};




TEST_F(SubscriberTest,TimeSubscriber) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);


  add_tuples(cube);
  cout << "added" <<endl;
  int tries = 0;
  while (cube->num_leaf_cells() < 3 && tries++ < 20)
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
  
//  ASSERT_EQ(3, cube->num_leaf_cells());
  //create subscriber
  
  
  
  //wait and check for data
  
//  add_tuples(cube);

  cout << "done" <<endl;
}