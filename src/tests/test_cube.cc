#include "cube_manager.h"
#include "cube.h"
#include "mysql/cube.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;

TEST(Cube, MysqlTest) {

  jetstream::CubeSchema * sc = new jetstream::CubeSchema();
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


  jetstream::Tuple t;
  jetstream::Element *e = t.add_e();
  e->set_t_val(time(NULL));
  e=t.add_e();
  e->set_s_val("http:\\\\www.example.com");
  e=t.add_e();
  e->set_i_val(200);
  e=t.add_e();
  e->set_i_val(50);

  cube->insert_entry(t);

  cout<<"created"<<endl;
}
