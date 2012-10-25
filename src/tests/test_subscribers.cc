

#include <iostream>
#include "cube_manager.h"
#include "node.h"

#include "base_subscribers.h"
#include "base_operators.h"
#include "experiment_operators.h"

#include "js_utils.h"


#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using namespace ::std;




const char * TEST_CUBE = "test_cube";
const int compID = 4;

class SubscriberTest : public ::testing::Test {
  protected:
  
  Node * node;


  virtual void SetUp() {

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
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    cout << "alter sent; cube should be present" << endl;
    
  }
  
  void add_tuples(shared_ptr<DataCube> cube) {

      boost::shared_ptr<Tuple> t(new Tuple);

      extend_tuple(*t, "http://foo.com");
      extend_tuple_time(*t, 10000); //long long ago
      extend_tuple(*t, 2);

      cout<< "Tuple:" << fmt(*t) << endl;
      cube->process(t);
  
    time_t now = time(NULL);
    for (int i =0; i < 3; ++i) {
      boost::shared_ptr<Tuple> t(new Tuple);

      extend_tuple(*t, "http://foo.com");
      extend_tuple_time(*t, now - i);
      extend_tuple(*t, i+1);

      cout<< "Tuple:" << fmt(*t) << endl;
      cube->process(t);
    }
  }
  
  //add a subscriber of typename subscriberName;
  // returns a pointer to the dummy operator
  shared_ptr<DummyReceiver>  start_time_subscriber(const string& subscriberName) {
  
    AlterTopo topo;
  
    TaskMeta* task = topo.add_tostart();
    TaskID * id = task->mutable_id();
    id->set_computationid(compID);
    id->set_task(1);
    task->set_op_typename(subscriberName);
    //TODO MORE CONFIG HERE
    TaskMeta_DictEntry* op_cfg = task->add_config();

    Tuple query_tuple;
    extend_tuple(query_tuple, "http://foo.com");
    extend_tuple_time(query_tuple, 0); //just a placeholder

    op_cfg = task->add_config();
    op_cfg->set_opt_name("slice_tuple");
    op_cfg->set_val(query_tuple.SerializeAsString());

    op_cfg = task->add_config();
    op_cfg->set_opt_name("ts_field");
    op_cfg->set_val("1");

    op_cfg = task->add_config();
    op_cfg->set_opt_name("start_ts");
    op_cfg->set_val("0");
    
    
    task = topo.add_tostart();
    id = task->mutable_id();
    id->set_computationid(compID);
    id->set_task(2);
    task->set_op_typename("DummyReceiver");


    Edge * e = topo.add_edges();
    e->set_src(1);
    e->set_dest(2);
    e->set_computation(compID);
    
    e = topo.add_edges();
    e->set_src_cube(TEST_CUBE);
    e->set_dest(1);
    e->set_computation(compID);
    
    
    ControlMessage r;
    node->handle_alter(r, topo);
    EXPECT_NE(r.type(), ControlMessage::ERROR);
    
    shared_ptr<DataPlaneOperator> dest = node->get_operator( operator_id_t(compID, 2));
    shared_ptr<DummyReceiver> rec = boost::static_pointer_cast<DummyReceiver>(dest);
    return rec;
  }
  
};




TEST_F(SubscriberTest,TimeSubscriber) {
  shared_ptr<DataCube> cube = node->get_cube(TEST_CUBE);


  add_tuples(cube);
  int tries = 0;
  while (cube->num_leaf_cells() < 3 && tries++ < 5)
    boost::this_thread::sleep(boost::posix_time::seconds(1));
  
  ASSERT_EQ(4U, cube->num_leaf_cells());
  //create subscriber
  shared_ptr<DummyReceiver> rec = start_time_subscriber("TimeBasedSubscriber");
  
  tries = 0;
  while (rec->tuples.size() < 4 && tries++ < 5)
    boost::this_thread::sleep(boost::posix_time::seconds(1));
  
  ASSERT_EQ(4U, rec->tuples.size()); //one very old, three newish
  
  
  //add more, wait and check for data
  boost::shared_ptr<Tuple> t(new Tuple);

  extend_tuple(*t, "http://foo.com");
  extend_tuple_time(*t, time(NULL));
  extend_tuple(*t, 2);
  cout<< "Tuple:" << fmt(*t) << endl;
  cube->process(t);

  boost::this_thread::sleep(boost::posix_time::seconds(1));
  
  while (rec->tuples.size() < 5 && tries++ < 5)
    boost::this_thread::sleep(boost::posix_time::seconds(1));

  ASSERT_EQ(5U, rec->tuples.size()); //update to old tuple should be suppressed
  
  cout << "done" <<endl;
}
