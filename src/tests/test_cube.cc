#include "cube_manager.h"
#include "node.h"

#include "mysql_cube.h"
#include "base_subscribers.h"
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
      dim->add_tuple_indexes(0);

      dim = sc->add_dimensions();
      dim->set_name("url");
      dim->set_type(Element_ElementType_STRING);
      dim->add_tuple_indexes(1);

      dim = sc->add_dimensions();
      dim->set_name("response_code");
      dim->set_type(Element_ElementType_INT32);
      dim->add_tuple_indexes(2);

      jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
      agg->set_name("count");
      agg->set_type("count");
      agg->add_tuple_indexes(4);

      agg = sc->add_aggregates();
      agg->set_name("avg_size");
      agg->set_type("avg");
      agg->add_tuple_indexes(3);
      agg->add_tuple_indexes(5);
    }

    jetstream::CubeSchema * sc;
  
  
   virtual void TearDown() {
      delete sc;
  }
};

/*TEST_F(CubeTest, MultiStatementTest) {
 * shows that you can't do multi-statements in prepared statements
 * later found this fact in the mysql manual
 *
  sql::Driver * driver = sql::mysql::get_driver_instance();

  string db_host="localhost";
  string db_user="root";
  string db_pass="";
  string db_name="test_cube";
  sql::ConnectOptionsMap options;
  options.insert( std::make_pair( "hostName", db_host));
  options.insert( std::make_pair( "userName", db_user));
  options.insert( std::make_pair( "password", db_pass));
  options.insert( std::make_pair( "CLIENT_MULTI_STATEMENTS", true ) );


  //shared_ptr<sql::Connection> con(driver->connect(db_host, db_user, db_pass));
  shared_ptr<sql::Connection> connection(driver->connect(options));
  connection->setSchema(db_name);

  shared_ptr<sql::Statement> stmnt(connection->createStatement());

  string sql = "select 1; select 2";
    try {
    stmnt->execute(sql);
  }
  catch (sql::SQLException &e) {
    LOG(WARNING) << "in test: couldn't execute sql statement; " << e.what() <<
                 "\nStatement was " << sql;
  }

  try {
      connection->prepareStatement(sql);
    }
    catch (sql::SQLException &e) {
      LOG(WARNING) << "in test ps: couldn't execute sql statement; " << e.what();
      LOG(WARNING) << "statement was " << sql;
    }
}*/

void insert_tuple(jetstream::Tuple & t, time_t time, string url, int rc, int sum, int count) {
  t.clear_e();
  jetstream::Element *e = t.add_e();
  e->set_t_val(time);  //0
  e=t.add_e();
  e->set_s_val(url);  //1
  e=t.add_e();
  e->set_i_val(rc);  //2
  e=t.add_e();
  e->set_i_val(sum);  //3
  e=t.add_e();
  e->set_i_val(count);  //4
  e=t.add_e();
  e->set_i_val(count);  //5
}

void check_tuple(boost::shared_ptr<jetstream::Tuple> const & answer, time_t time, string url, int rc, int sum, int count) {
  ASSERT_EQ(time, answer->e(0).t_val());
  ASSERT_STREQ(url.c_str(), answer->e(1).s_val().c_str());
  ASSERT_EQ(rc, answer->e(2).i_val());
  ASSERT_EQ(count, answer->e(3).i_val());
  ASSERT_EQ(sum, answer->e(4).i_val());
  ASSERT_EQ(sum, answer->e(4).d_val());
  ASSERT_EQ(count, answer->e(5).i_val());
}

void check_tuple_input(boost::shared_ptr<jetstream::Tuple> const & answer, time_t time, string url, int rc, int sum, int count) {
  ASSERT_EQ(time, answer->e(0).t_val());
  ASSERT_STREQ(url.c_str(), answer->e(1).s_val().c_str());
  ASSERT_EQ(rc, answer->e(2).i_val());
  ASSERT_EQ(sum, answer->e(3).i_val());
  ASSERT_EQ(count, answer->e(4).i_val());
  ASSERT_EQ(count, answer->e(5).i_val());
}

TEST_F(CubeTest, SaveTupleTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
//  cube->destroy();
  cube->create();

  jetstream::Tuple t;
  time_t time_entered = time(NULL);
  insert_tuple(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  cube->save_tuple(t, false, false, new_tuple, old_tuple);
  ASSERT_FALSE(new_tuple);
  ASSERT_FALSE(old_tuple);
  cube->save_tuple(t, true, true, new_tuple, old_tuple);
  ASSERT_TRUE(new_tuple);
  ASSERT_TRUE(old_tuple);
  check_tuple(old_tuple, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  check_tuple(new_tuple, time_entered, "http:\\\\www.example.com", 200, 100, 2);
  cube->save_tuple(t, false, false, new_tuple, old_tuple);
  ASSERT_FALSE(new_tuple);
  ASSERT_FALSE(old_tuple);
  delete cube;
}

TEST_F(CubeTest, SavePartialTupleTest) {
  MysqlCube * cube;
  try {
    cube = new MysqlCube(*sc, "web_requests", true);
  } catch (boost::system::system_error e) {
    LOG(FATAL) << e.what();
  }
//  cube->destroy();
  cube->create();

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
  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  cube->save_tuple(t, true, false, new_tuple, old_tuple);
  ASSERT_FALSE(old_tuple);
  ASSERT_TRUE(new_tuple);
  check_tuple(new_tuple, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  delete cube;
}


TEST_F(CubeTest, SaveTupleBatchTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t1 = boost::make_shared<jetstream::Tuple>();
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();

  time_t time_entered = time(NULL);
  insert_tuple(*t1, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);

  std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store;
  tuple_store.push_back(t1);
  tuple_store.push_back(t2);

  vector<bool> all_true;
  all_true.push_back(true);
  all_true.push_back(true);
  vector<bool> all_false;
  all_false.push_back(false);
  all_false.push_back(false);
  
  std::list<boost::shared_ptr<jetstream::Tuple> > new_tuple_list;
  std::list<boost::shared_ptr<jetstream::Tuple> > old_tuple_list;

  cube->save_tuple_batch(tuple_store, all_true, all_false, new_tuple_list, old_tuple_list);
  ASSERT_EQ(2U, new_tuple_list.size());
  ASSERT_EQ(0U, old_tuple_list.size());
  check_tuple((new_tuple_list.front()), time_entered, "http:\\\\www.example.com", 200, 50, 1);
  check_tuple((new_tuple_list.back()), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  delete cube;
}

TEST_F(CubeTest, SubscriberTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(t);
  
  for(int i =0; i < 100 &&  sub->insert_q.size() < 1; i++)
  {
    js_usleep(100000); 
  }
  ASSERT_EQ(1U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 50, 1);

  sub->returnAction = Subscriber::SEND_UPDATE;
  cube->process(t);

  for(int i =0; i < 100 &&  sub->update_q.size() < 1; i++)
  {
    js_usleep(100000); 
  }
  ASSERT_EQ(1U, sub->update_q.size());
  check_tuple(sub->update_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  delete cube;
}
/* method now protected
TEST_F(CubeTest, MergeTupleIntoTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->merge_tuple_into(*t,*t2);
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);


} */


TEST_F(CubeTest, SubscriberBatchTestInsertInsert) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();

  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  
  cube->process(t); 
  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(t);

  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++)
  {
    js_usleep(100000); 
  }
  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2); 
  
  for(int i =0; i < 100 &&  sub->insert_q.size() < 2; i++)
  {
    js_usleep(10000); 
  }
  ASSERT_EQ(2U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  check_tuple(sub->insert_q.back(), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cout << "done" <<endl;
  delete cube;
}

TEST_F(CubeTest, SubscriberBatchTestUpdateUpdate) {
  MysqlCube  * cube;
  try {
    cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  } catch (boost::system::system_error e) {
    LOG(FATAL) << e.what();
  }
  cout << "created cube; adding subscriber " << endl;

  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  sub->returnAction = Subscriber::SEND_UPDATE;
  js_usleep(1000000);
  cube->process(t); 
  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(0U, sub->update_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(t);
  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(0U, sub->update_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2); 
  for(int i =0; i < 100 &&  sub->update_q.size() < 2; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(2U, sub->update_q.size());
  check_tuple(sub->update_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  check_tuple(sub->update_q.back(), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cout << "done" <<endl;
  delete cube;
}



TEST_F(CubeTest, SubscriberBatchTestInsertUpdate) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  cube->set_batch_timeout( boost::posix_time::seconds(10));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(t); 
  
  ASSERT_EQ(0U, sub->insert_q.size());
  js_usleep(1000000);
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_UPDATE;
  cube->process(t);
  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2); 
  for(int i =0; i < 5 && (sub->update_q.size() < 1 || sub->insert_q.size() < 1); i++)
  {
    VLOG(1)<< "."<<i;
    js_usleep(1000000);
  }
  ASSERT_EQ(1U, sub->insert_q.size());
  ASSERT_EQ(1U, sub->update_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  check_tuple(sub->update_q.front(), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cout << "done" <<endl;
  delete cube;
}

TEST_F(CubeTest, SubscriberNoBatch) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_NO_BATCH;
  cube->process(t); 
  for(int i =0; i < 100 &&  sub->insert_q.size() < 1; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(1U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 50, 1);
  delete cube;
}

TEST_F(CubeTest, SubscriberBatchInsertNoBatch) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(t); 
  for(int i =0; i < 100 &&  cube->batch_size() < 1; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_NO_BATCH;
  cube->process(t);
  for(int i =0; i < 100 &&  sub->insert_q.size() < 2; i++)
  {
    js_usleep(100000);
  }
  ASSERT_EQ(1U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);


  sub->returnAction = Subscriber::SEND;
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2); 
  ASSERT_EQ(1U, sub->insert_q.size());
  boost::shared_ptr<jetstream::Tuple> t3 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t3, time_entered, "http:\\\\www.example.com", 202, 50, 1);
  cube->process(t3);
  for(int i =0; i < 5 &&  sub->insert_q.size() < 3; i++)
  {
    js_usleep(1000000);
  }
  ASSERT_EQ(3U, sub->insert_q.size());
  check_tuple(sub->insert_q.back(), time_entered, "http:\\\\www.example.com", 202, 50, 1);

  delete cube;
}

TEST_F(CubeTest, SubscriberBatchTimeout) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  cube->set_batch_timeout( boost::posix_time::seconds(1));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND;
  cube->process(t);
  js_usleep(500000);
  ASSERT_EQ(0U, sub->insert_q.size());
  js_usleep(2000000);
  ASSERT_EQ(1U, sub->insert_q.size());

  delete cube;
}

TEST_F(CubeTest, SubscriberBatchCountTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 2);
  cube->set_batch_timeout( boost::posix_time::seconds(10));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND;
  cube->process(t);
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2);
  js_usleep(500000);
  ASSERT_EQ(2U, cube->num_leaf_cells() );
  ASSERT_EQ(2U, sub->insert_q.size());
  delete cube;
}

TEST_F(CubeTest, SubscriberBatchChangeCountTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true,"localhost", "root", "", "test_cube", 1);
  cube->set_elements_in_batch(2);
  cube->set_batch_timeout( boost::posix_time::seconds(10));
  cube->destroy();
  cube->create();
  
  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(t);
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(t2);
  js_usleep(500000);
  ASSERT_EQ(2U, cube->num_leaf_cells() );
  delete cube;
}





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
  time_t time_entered = time(NULL);
  insert_tuple(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  cube->save_tuple(t, false, false, new_tuple, old_tuple);
  ASSERT_EQ(1U, cube->num_leaf_cells());

  jetstream::Element *e;
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
  cube->save_tuple(t, false, false, new_tuple, old_tuple);
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


  delete cube;
}


TEST_F(CubeTest, MysqlTestIt) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

  time_t time_entered = time(NULL);


  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 300, 2);
  cube->process(t);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 300, 300, 2);
  cube->process(t2);

  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 2; i++)
  {
    js_usleep(1000000);
  }

  jetstream::Element *e;
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

  time_t time_entered = time(NULL);

  list<int> rscs;
  rscs.push_back(100);
  rscs.push_back(200);
  rscs.push_back(300);
  rscs.push_back(400);
  rscs.push_back(500);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
     boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
     insert_tuple(*t, time_entered, "http:\\\\www.example.com", *i, 300, 2);
     cube->process(t);
  }

  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 5; i++)
  {
    js_usleep(1000000);
  }
  jetstream::Element *e;
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
  string cubeName = "text_and_count";

  jetstream::CubeMeta * cube_meta = topo.add_tocreate();
  cube_meta->set_name(cubeName);
  cube_meta->set_overwrite_old(true);

  jetstream::CubeSchema * sc = cube_meta->mutable_schema();

  jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
  dim->set_type(Element_ElementType_STRING);
  dim->set_name("text");
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  agg->add_tuple_indexes(1);

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
  e->set_dest_cube(cubeName);
  e->set_computation(compID);

//  cout << topo.Utf8DebugString();

  ControlMessage r;
  node.handle_alter(r, topo);
  cout << "alter sent; data should be present" << endl;

  shared_ptr<DataCube> cube = node.get_cube(cubeName);
  ASSERT_TRUE( cube );
  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 1; i++)
  {
    js_usleep(1000000);
  }
  ASSERT_EQ(1U, cube->num_leaf_cells());
  Tuple empty = cube->empty_tuple();

  cube::CubeIterator it = cube->slice_query(empty, empty);
  for(int i =0; i < 5 &&  (it.numCells() < 1 || (*it)->e_size() < 2); i++)
  {
    js_usleep(1000000);
    it = cube->slice_query(empty, empty);
  }

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

  time_t time_entered = time(NULL);

  list<int> rscs;
  rscs.push_back(100);
  rscs.push_back(200);
  rscs.push_back(300);
  rscs.push_back(400);
  rscs.push_back(500);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
     boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
     insert_tuple(*t, time_entered, "http:\\\\www.example.com", *i, *i, *i/100);
     cube->process(t);
  }
  
  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 5; i++)
  {
    js_usleep(1000000);
  }

  jetstream::Element *e;
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
//  Disabled because we ren't using the time hierarchy right now.
TEST_F(CubeTest, DISABLED_MysqlTestTimeRollup) {

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);

  cube->destroy();
  cube->create();

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
     boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
     insert_tuple(*t, time_entered+(*i), "http:\\\\www.example.com", *i, 500, 100);
     cube->process(t);
  }
  
  for(int i =0; i < 4 &&  cube->num_leaf_cells() < 5; i++)
  {
    js_usleep(1000000);
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
