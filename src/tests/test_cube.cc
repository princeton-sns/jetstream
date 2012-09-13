#include "cube_manager.h"
#include "cube.h"
#include "mysql_cube.h"
#include "mysql/cube_iterator_impl.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;

class CubeTest : public ::testing::Test {
  protected:
    virtual void SetUp() {

      sc = new jetstream::CubeSchema();
      sc->set_name("web_requests");

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



  MysqlCube * cube = new MysqlCube(*sc);
  vector<std::string> test_strings = cube->get_dimension_column_types();
 
  ASSERT_STREQ("DATETIME",test_strings[0].c_str());
  ASSERT_STREQ("VARCHAR(255)",test_strings[1].c_str());
  ASSERT_STREQ("INT",test_strings[2].c_str());


  test_strings = cube->get_aggregate_column_types();
  ASSERT_STREQ("INT",test_strings[0].c_str());
  ASSERT_STREQ("INT",test_strings[1].c_str());
  ASSERT_STREQ("INT",test_strings[2].c_str());
  /*for (size_t i = 0; i < test_strings.size(); i++) {
    cout << test_strings[i] <<endl;
  }*/

  ASSERT_STREQ("CREATE TABLE `web_requests` (`time` DATETIME NOT NULL,`url` VARCHAR(255) NOT NULL,`response_code` INT NOT NULL,`count` INT DEFAULT NULL,`avg_size_sum` INT DEFAULT NULL,`avg_size_count` INT DEFAULT NULL,PRIMARY KEY (`time`, `url`, `response_code`)) ENGINE=MyISAM", cube->create_sql().c_str());

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
  
  MysqlCube * cube_batch = new MysqlCube(*sc);
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

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc);

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

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc);

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
