#include "cube_manager.h"
#include "node.h"

#include "mysql_cube.h"
#include "cube_iterator_impl.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;

class CubeTest : public ::testing::Test {
  protected:
    virtual void SetUp() {

      sc = new jetstream::CubeSchema();

      jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
      dim->set_name("time");
      dim->set_type(Element_ElementType_TIME);

      dim = sc->add_dimensions();
      dim->set_name("url");
      dim->set_type(Element_ElementType_STRING);

      dim = sc->add_dimensions();
      dim->set_name("response_code");
      dim->set_type(Element_ElementType_INT32);

      jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
      agg->set_name("count");
      agg->set_type("count");

      agg = sc->add_aggregates();
      agg->set_name("avg_size");
      agg->set_type("avg");

    }
    
    jetstream::CubeSchema * sc;
};


TEST_F(CubeTest, MysqlTest) {



  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  vector<std::string> test_strings = cube->get_dimension_column_types();
 
  /*ASSERT_STREQ("DATETIME",test_strings[0].c_str());
  ASSERT_STREQ("VARCHAR(255)",test_strings[1].c_str());
  ASSERT_STREQ("INT",test_strings[2].c_str());


  test_strings = cube->get_aggregate_column_types();
  ASSERT_STREQ("INT",test_strings[0].c_str());
  ASSERT_STREQ("INT",test_strings[1].c_str());
  ASSERT_STREQ("INT",test_strings[2].c_str());

  ASSERT_STREQ("CREATE TABLE IF NOT EXISTS `web_requests` (`time` DATETIME NOT NULL,`url` VARCHAR(255) NOT NULL,`response_code` INT NOT NULL,`count` INT DEFAULT NULL,`avg_size_sum` INT DEFAULT NULL,`avg_size_count` INT DEFAULT NULL,PRIMARY KEY (`time`, `url`, `response_code`)) ENGINE=MyISAM", cube->create_sql().c_str());
*/
  cube->destroy();
  cube->create();

  ASSERT_EQ(0U, cube->num_leaf_cells());

  jetstream::Tuple t;
  jetstream::Element *e = t.add_e();
  time_t time_entered = time(NULL);
  e->set_t_val(time_entered);
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(200);
  e=t.add_e();
  e->set_i_val(50);
 
  cube->insert_entry(t);
  ASSERT_EQ(1U, cube->num_leaf_cells());

  jetstream::Tuple query;
  e = query.add_e();
  e->set_t_val(time_entered);
  e=query.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=query.add_e();
  e->set_i_val(200);

  boost::shared_ptr<jetstream::Tuple> answer = cube->get_cell_value_final(query);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(1, answer->e(3).i_val());
  ASSERT_EQ(50, answer->e(4).i_val());
  ASSERT_EQ(50, answer->e(4).d_val());
  
  answer = cube->get_cell_value_partial(query);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(1, answer->e(3).i_val());
  ASSERT_EQ(50, answer->e(4).i_val());
  ASSERT_EQ(1, answer->e(5).i_val());



  e = t.mutable_e(3);
  e->set_i_val(100);
  cube->insert_entry(t);
  ASSERT_EQ(1U, cube->num_leaf_cells());

  answer = cube->get_cell_value_final(query);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(2, answer->e(3).i_val());
  ASSERT_EQ(75, answer->e(4).i_val());
  ASSERT_EQ(75, answer->e(4).d_val());

  answer = cube->get_cell_value_partial(query);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(2, answer->e(3).i_val());
  ASSERT_EQ(150, answer->e(4).i_val());
  ASSERT_EQ(2, answer->e(5).i_val());
  
  MysqlCube * cube_batch = new MysqlCube(*sc, "web_requests", true);
  cube_batch->set_batch(2);

  cube_batch->destroy();
  cube_batch->create();


  cube_batch->insert_entry(t);
  answer = cube_batch->get_cell_value_final(query);
  ASSERT_FALSE(answer);
  cube_batch->insert_entry(t);
  answer = cube_batch->get_cell_value_final(query);
  ASSERT_TRUE(answer);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(2, answer->e(3).i_val());
  ASSERT_EQ(100, answer->e(4).i_val());
  ASSERT_EQ(100, answer->e(4).d_val());


  t.clear_e();
  e = t.add_e();
  e->set_t_val(time_entered);
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(200);
  //aggregate values
  e=t.add_e();
  e->set_i_val(2);
  e=t.add_e();
  e->set_i_val(300);
  e=t.add_e();
  e->set_i_val(2);

  cube->insert_partial_aggregate(t);
  ASSERT_EQ(1U, cube->num_leaf_cells());

  answer = cube_batch->get_cell_value_final(query);
  ASSERT_TRUE(answer);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(4, answer->e(3).i_val());
  ASSERT_EQ(125, answer->e(4).i_val());
  ASSERT_EQ(125, answer->e(4).d_val());

  t.clear_e();
  e = t.add_e();
  e->set_t_val(time_entered);
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(300);
  //aggregate values
  e=t.add_e();
  e->set_i_val(2);
  e=t.add_e();
  e->set_i_val(300);
  e=t.add_e();
  e->set_i_val(2);

  cube->insert_partial_aggregate(t);
  ASSERT_EQ(2U, cube->num_leaf_cells());
}


TEST_F(CubeTest, MysqlTestIt) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

  jetstream::Tuple t;
  jetstream::Element *e;
  time_t time_entered = time(NULL);

  t.clear_e();
  e = t.add_e();
  e->set_t_val(time_entered);
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(200);
  //aggregate values
  e=t.add_e();
  e->set_i_val(2);
  e=t.add_e();
  e->set_i_val(300);
  e=t.add_e();
  e->set_i_val(2);

  cube->insert_partial_aggregate(t);

  t.clear_e();
  e = t.add_e();
  e->set_t_val(time_entered);
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(300);
  //aggregate values
  e=t.add_e();
  e->set_i_val(2);
  e=t.add_e();
  e->set_i_val(300);
  e=t.add_e();
  e->set_i_val(2);

  cube->insert_partial_aggregate(t);


  jetstream::Tuple max;
  e=max.add_e(); //time
  e=max.add_e(); //url
  e=max.add_e(); //rc
 
  CubeIterator it = cube->slice_query(max, max, true);

  ASSERT_EQ((size_t)2, it.numCells());
  ASSERT_FALSE(it == cube->end());

  boost::shared_ptr<Tuple> ptrTup;
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());

  ++it;
  ASSERT_FALSE(it == cube->end());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());

  ++it;
  ptrTup = *it;
  ASSERT_FALSE(ptrTup);
  ASSERT_TRUE(it == cube->end());
  
  /*it = cube->slice_query(max, max, true);
  std::copy(
        it, cube->end()
      , std::ostream_iterator<boost::shared_ptr<jetstream::Tuple> >(std::cout, " ")
    );
  */

  max.clear_e();
  e=max.add_e(); //time
  e=max.add_e(); //url
  e=max.add_e(); //rc
  e->set_i_val(350);

  jetstream::Tuple min;
  e=min.add_e(); //time
  e=min.add_e(); //url
  e=min.add_e(); //rc
  e->set_i_val(250);
  
  it = cube->slice_query(min, max, true);
  ASSERT_EQ((size_t)1, it.numCells());

  e=min.mutable_e(2);
  e->set_i_val(200);

  it = cube->slice_query(min, max, true);
  ASSERT_EQ((size_t)2, it.numCells());

  max.clear_e();
  e=max.add_e(); //time
  e=max.add_e(); //url
  e=max.add_e(); //rc
  e->set_i_val(150);

  min.clear_e();
  e=min.add_e(); //time
  e=min.add_e(); //url
  e=min.add_e(); //rc
  e->set_i_val(100);
  
  it = cube->slice_query(min, max, true);
  ASSERT_EQ((size_t)0, it.numCells());
  ptrTup = *it;
  ASSERT_FALSE(ptrTup);
}

TEST_F(CubeTest, MysqlTestSort) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

  jetstream::Tuple t;
  jetstream::Element *e;
  time_t time_entered = time(NULL);

  list<int> rscs;
  rscs.push_back(100);
  rscs.push_back(200);
  rscs.push_back(300);
  rscs.push_back(400);
  rscs.push_back(500);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
    t.clear_e();
    e = t.add_e();
    e->set_t_val(time_entered);
    e=t.add_e();
    e->set_s_val("http:\\\\www.example.com");
    e=t.add_e();
    e->set_i_val(*i);
    //aggregate values
    e=t.add_e();
    e->set_i_val(2);
    e=t.add_e();
    e->set_i_val(300);
    e=t.add_e();
    e->set_i_val(2);

    cube->insert_partial_aggregate(t);
  }
  jetstream::Tuple max;
  e=max.add_e(); //time
  e=max.add_e(); //url
  e=max.add_e(); //rc

  list<string> sort;
  sort.push_back("response_code");

  CubeIterator it = cube->slice_query(max, max, true, sort);
  ASSERT_EQ((size_t)5, it.numCells());

  boost::shared_ptr<Tuple> ptrTup;
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(100, ptrTup->e(2).i_val());
  
  it = cube->slice_query(max, max, true, sort, 1);
  ASSERT_EQ((size_t)1, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(100, ptrTup->e(2).i_val());

  sort.clear();
  sort.push_back("-response_code");
  
  it = cube->slice_query(max, max, true, sort);
  ASSERT_EQ((size_t)5, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(500, ptrTup->e(2).i_val());

  it = cube->slice_query(max, max, true, sort, 3);
  ASSERT_EQ((size_t)3, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(500, ptrTup->e(2).i_val());

}

TEST(Cube,Attach) {
  int compID = 4;

  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  ASSERT_TRUE(error == 0);

  AlterTopo topo;
  topo.set_computationid(compID);
  
  jetstream::CubeMeta * cube_meta = topo.add_tocreate();
  cube_meta->set_name("text");

  jetstream::CubeSchema * sc = cube_meta->mutable_schema();
  
  jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
  dim->set_type(Element_ElementType_STRING);
  dim->set_name("text");  
  
  jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  cube_meta->set_name("test_cube");
  cube_meta->set_overwrite_old(true);

  TaskMeta* task = topo.add_tostart();
  task->set_op_typename("SendK");
  // Send some tuples
  TaskMeta_DictEntry* op_cfg = task->add_config();
  op_cfg->set_opt_name("k");
  op_cfg->set_val("2");
  
  op_cfg = task->add_config();
  op_cfg->set_opt_name("send_now");
  op_cfg->set_val("true");
  
  TaskID* id = task->mutable_id();
  id->set_computationid(compID);
  id->set_task(1);
  
  Edge * e = topo.add_edges();
  e->set_src(1);
  e->set_cube_name("test_cube");
  e->set_computation(compID);
  
//  cout << topo.Utf8DebugString();
  
  ControlMessage r;
  node.handle_alter(r, topo);
  cout << "alter sent; data should be present" << endl;
  
  shared_ptr<DataCube> cube = node.get_cube("test_cube");
  ASSERT_TRUE( cube );
  ASSERT_EQ(1U, cube->num_leaf_cells());
  Tuple empty = cube->empty_tuple();
  cube::CubeIterator it = cube->slice_query(empty, empty);
  ASSERT_EQ(1U, it.numCells());
  int total_count = 0;
  shared_ptr<Tuple> t = *it;
  ASSERT_EQ(2, t->e_size());
  total_count += t->e(1).i_val();

  ASSERT_EQ(2, total_count);
  cout << "done"<< endl;
  node.stop();
 
}

TEST_F(CubeTest, MysqlTestFlatRollup) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

  jetstream::Tuple t;
  jetstream::Element *e;
  time_t time_entered = time(NULL);

  list<int> rscs;
  rscs.push_back(100);
  rscs.push_back(200);
  rscs.push_back(300);
  rscs.push_back(400);
  rscs.push_back(500);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
    t.clear_e();
    e = t.add_e();
    e->set_t_val(time_entered);
    e=t.add_e();
    e->set_s_val("http:\\\\www.example.com");
    e=t.add_e();
    e->set_i_val(*i);
    //aggregate values
    e=t.add_e();
    e->set_i_val(*i/100);
    e=t.add_e();
    e->set_i_val(*i);
    e=t.add_e();
    e->set_i_val(*i/100);

    cube->insert_partial_aggregate(t);
  }

  jetstream::Tuple empty;
  e=empty.add_e(); //time
  e=empty.add_e(); //url
  e=empty.add_e(); //rc
 
  list<unsigned int> levels;
  levels.push_back(MysqlDimensionTimeHierarchy::LEVEL_SECOND);
  levels.push_back(1);
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, empty, empty);
  ASSERT_EQ(1U, it.numCells());
  boost::shared_ptr<Tuple> ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_SECOND, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(2).s_val().c_str());
  ASSERT_EQ(1, ptrTup->e(3).i_val());
  ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(0, ptrTup->e(5).i_val());
  ASSERT_EQ(15, ptrTup->e(6).i_val());
  ASSERT_EQ(100, ptrTup->e(7).i_val());


  //doesn't exist

  jetstream::Tuple max;
  e=max.add_e(); //time
  e->set_t_val(50);
  e=max.add_e(); //url
  e=max.add_e(); //rc
  cube->do_rollup(levels, empty, max);

  it = cube->rollup_slice_query(levels, empty, max);
  ASSERT_EQ(0U, it.numCells());


}

TEST_F(CubeTest, MysqlTestTimeRollup) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

  jetstream::Tuple t;
  jetstream::Element *e;

  //make it even on the minute
  time_t time_entered = time(NULL); 
  struct tm temptm;
  localtime_r(&time_entered, &temptm);
  temptm.tm_sec=0;
  time_entered = mktime(&temptm);

  list<int> rscs;
  rscs.push_back(1);
  rscs.push_back(2);
  rscs.push_back(60*60+1);
  rscs.push_back(60*60+1);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
    t.clear_e();
    e = t.add_e();
    e->set_t_val(time_entered+(*i));
    e=t.add_e();
    e->set_s_val("http:\\\\www.example.com");
    e=t.add_e();
    e->set_i_val(*i);
    //aggregate values
    e=t.add_e();
    e->set_i_val(100);
    e=t.add_e();
    e->set_i_val(500);
    e=t.add_e();
    e->set_i_val(100);

    cube->insert_partial_aggregate(t);
  }

  jetstream::Tuple empty;
  e=empty.add_e(); //time
  e=empty.add_e(); //url
  e=empty.add_e(); //rc
  
  jetstream::Tuple max;
  e=max.add_e(); //time
  e->set_t_val(time_entered+1);
  e=max.add_e(); //url
  e=max.add_e(); //rc
 
  list<unsigned int> levels;
  levels.push_back(MysqlDimensionTimeHierarchy::LEVEL_SECOND);
  levels.push_back(1);
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, max, max);
  ASSERT_EQ(1U, it.numCells());
  boost::shared_ptr<Tuple> ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(time_entered+1, ptrTup->e(0).t_val());
  ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_SECOND, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(2).s_val().c_str());
  ASSERT_EQ(1, ptrTup->e(3).i_val());
  ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(0, ptrTup->e(5).i_val());
  ASSERT_EQ(100, ptrTup->e(6).i_val());
  ASSERT_EQ(5, ptrTup->e(7).i_val());

  levels.clear(); 
  levels.push_back(MysqlDimensionTimeHierarchy::LEVEL_MINUTE);
  levels.push_back(1);
  levels.push_back(0);

   max.clear_e();
   e=max.add_e(); //time
  e->set_t_val(time_entered);
  e=max.add_e(); //url
  e=max.add_e(); //rc


  cube->do_rollup(levels, empty, empty);
  it = cube->rollup_slice_query(levels, max, max);
  ASSERT_EQ(1U, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup);
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_MINUTE, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(2).s_val().c_str());
  ASSERT_EQ(1, ptrTup->e(3).i_val());
  ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(0, ptrTup->e(5).i_val());
  ASSERT_EQ(200, ptrTup->e(6).i_val());
  ASSERT_EQ(5, ptrTup->e(7).i_val());

}
