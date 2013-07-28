#include "js_utils.h"

#include "cube_manager.h"
#include "node.h"

#include "mysql_cube.h"
#include "base_subscribers.h"
#include "cube_iterator_impl.h"
#include "quantile_est.h"
//#include "time_rollup_manager.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
using std::endl;
using std::cout;

class CubeTest : public ::testing::Test {
  protected:
    virtual void SetUp() {

      sc = new jetstream::CubeSchema();

      jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
      dim->set_name("time");
      dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
      dim->add_tuple_indexes(0);

      dim = sc->add_dimensions();
      dim->set_name("url");
      dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
      dim->add_tuple_indexes(1);

      dim = sc->add_dimensions();
      dim->set_name("response_code");
      dim->set_type(CubeSchema_Dimension_DimensionType_INT32);
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

      MysqlCube::set_db_params("localhost", "root", "", "test_cube");
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

void check_tuple_(boost::shared_ptr<jetstream::Tuple> const & answer, time_t time, string url, int rc, int sum, int count) {
  ASSERT_EQ(time, answer->e(0).t_val());
  ASSERT_STREQ(url.c_str(), answer->e(1).s_val().c_str());
  ASSERT_EQ(rc, answer->e(2).i_val());
  ASSERT_EQ(count, answer->e(3).i_val());
  ASSERT_EQ(sum, answer->e(4).i_val());
  ASSERT_EQ(sum, answer->e(4).d_val());
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
  ASSERT_TRUE(new_tuple.get());
  ASSERT_TRUE(old_tuple.get());
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
  }
  catch (boost::system::system_error e) {
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
  ASSERT_TRUE(new_tuple.get());
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

  boost::shared_ptr<jetstream::Tuple> empty_ptr;
  std::vector<boost::shared_ptr<jetstream::Tuple> > new_tuple_store(tuple_store.size(), empty_ptr);
  std::vector<boost::shared_ptr<jetstream::Tuple> > old_tuple_store(tuple_store.size(), empty_ptr);

  cube->save_tuple_batch(tuple_store, all_true, all_false, new_tuple_store, old_tuple_store);

  ASSERT_EQ(2U, new_tuple_store.size());
  ASSERT_EQ(2U, old_tuple_store.size());
  check_tuple((new_tuple_store[0]), time_entered, "http:\\\\www.example.com", 200, 50, 1);
  check_tuple((new_tuple_store[1]), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  delete cube;
}



TEST_F(CubeTest, ProcessTest) {
  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);
  //MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(NULL, t);
  cube->wait_for_commits();

  ASSERT_EQ(1U, cube->num_leaf_cells());  //we sent several tuples that should have been aggregated
  Tuple empty = cube->empty_tuple();
  cube::CubeIterator it = cube->slice_query(empty, empty, false);
  ASSERT_EQ(1U, it.numCells());
  boost::shared_ptr<Tuple> ret = *it;
  check_tuple(ret, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  ASSERT_EQ(0U, ret->version());

  t = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(NULL, t);
  cube->wait_for_commits();
  it = cube->slice_query(empty, empty, false);
  ASSERT_EQ(1U, it.numCells());
  ret = *it;
  check_tuple(ret, time_entered, "http:\\\\www.example.com", 200, 100, 2 );
  ASSERT_EQ(1U, ret->version());

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

  cube->process(NULL, t);

  for(int i =0; i < 100 &&  sub->insert_q.size() < 1; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(1U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 50, 1);

  sub->returnAction = Subscriber::SEND_UPDATE;
  cube->process(NULL, t);

  for(int i =0; i < 100 &&  sub->update_q.size() < 1; i++) {
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


TEST_F(CubeTest, DISABLED_SubscriberBatchTestInsertInsert) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();

  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(NULL, t);
  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(NULL, t);

  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(NULL, t2);

  for(int i =0; i < 100 &&  sub->insert_q.size() < 2; i++) {
    js_usleep(10000);
  }

  ASSERT_EQ(2U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  check_tuple(sub->insert_q.back(), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cout << "done" <<endl;
  delete cube;
}

TEST_F(CubeTest, DISABLED_SubscriberBatchTestUpdateUpdate) {
  MysqlCube  * cube;

  try {
    cube = new MysqlCube(*sc, "web_requests", true);
  }
  catch (boost::system::system_error e) {
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
  cube->process(NULL, t);
  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(0U, sub->update_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(NULL, t);

  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(0U, sub->update_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(NULL, t2);

  for(int i =0; i < 100 &&  sub->update_q.size() < 2; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(0U, sub->insert_q.size());
  ASSERT_EQ(2U, sub->update_q.size());
  check_tuple(sub->update_q.front(), time_entered, "http:\\\\www.example.com", 200, 100, 2);
  check_tuple(sub->update_q.back(), time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cout << "done" << endl;
  delete cube;
}



TEST_F(CubeTest, DISABLED_SubscriberBatchTestInsertUpdate) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  //cube->set_batch_timeout( boost::posix_time::seconds(10));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(NULL, t);

  ASSERT_EQ(0U, sub->insert_q.size());
  js_usleep(1000000);
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_UPDATE;
  cube->process(NULL, t);

  for(int i =0; i < 100 && t->e(4).i_val() < 2 ; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 100, 2);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(NULL, t2);

  for(int i =0; i < 5 && (sub->update_q.size() < 1 || sub->insert_q.size() < 1); i++) {
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
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_NO_BATCH;
  cube->process(NULL, t);

  for(int i =0; i < 100 &&  sub->insert_q.size() < 1; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(1U, sub->insert_q.size());
  check_tuple(sub->insert_q.front(), time_entered, "http:\\\\www.example.com", 200, 50, 1);
  delete cube;
}
/*
TEST_F(CubeTest, DISABLED_SubscriberBatchInsertNoBatch) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  //cube->set_batch_timeout( boost::posix_time::seconds(1);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);

  cube->process(t);

  for(int i =0; i < 100 &&  cube->batch_size() < 1; i++) {
    js_usleep(100000);
  }

  ASSERT_EQ(0U, sub->insert_q.size());
  check_tuple_input(t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND_NO_BATCH;
  cube->process(t);

  for(int i =0; i < 100 &&  sub->insert_q.size() < 1; i++) {
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

  for(int i =0; i < 5 &&  sub->insert_q.size() < 3; i++) {
    js_usleep(1000000);
  }

  ASSERT_EQ(3U, sub->insert_q.size());
  check_tuple(sub->insert_q.back(), time_entered, "http:\\\\www.example.com", 202, 50, 1);

  delete cube;
}
*/


TEST_F(CubeTest, DISABLED_SubscriberBatchTimeout) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  //cube->set_batch_timeout( boost::posix_time::seconds(1));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND;
  cube->process(NULL, t);
  js_usleep(500000);
  ASSERT_EQ(0U, sub->insert_q.size());
  js_usleep(1000000);
  ASSERT_EQ(1U, sub->insert_q.size());

  delete cube;
}

TEST_F(CubeTest, SubscriberBatchCountTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  //cube->set_batch_timeout( boost::posix_time::seconds(10));
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  sub->returnAction = Subscriber::SEND;
  cube->process(NULL, t);
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(NULL, t2);
  js_usleep(100000);
  ASSERT_EQ(2U, cube->num_leaf_cells() );
  ASSERT_EQ(2U, sub->insert_q.size());
  delete cube;
}

TEST_F(CubeTest, SubscriberBatchChangeCountTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  //cube->set_elements_in_batch(2);
  //cube->set_batch_timeout( boost::posix_time::seconds(10));
  cube->destroy();
  cube->create();

  boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(NULL, t);
  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 201, 50, 1);
  cube->process(NULL, t2);
  js_usleep(100000);
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

  boost::shared_ptr<jetstream::Tuple> answer = cube->get_cell_value(query, *(cube->get_leaf_levels()), true);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(1, answer->e(3).i_val());
  ASSERT_EQ(50, answer->e(4).i_val());
  ASSERT_EQ(50, answer->e(4).d_val());

  answer = cube->get_cell_value(query,*(cube->get_leaf_levels()), false);
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

  answer = cube->get_cell_value(query,*(cube->get_leaf_levels()), true);
  ASSERT_EQ(time_entered, answer->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", answer->e(1).s_val().c_str());
  ASSERT_EQ(200, answer->e(2).i_val());
  ASSERT_EQ(2, answer->e(3).i_val());
  ASSERT_EQ(75, answer->e(4).i_val());
  ASSERT_EQ(75, answer->e(4).d_val());

  answer = cube->get_cell_value(query, *(cube->get_leaf_levels()), false);
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
  cube->process(NULL, t);

  boost::shared_ptr<jetstream::Tuple> t2 = boost::make_shared<jetstream::Tuple>();
  insert_tuple(*t2, time_entered, "http:\\\\www.example.com", 300, 300, 2);
  cube->process(NULL, t2);

  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 2; i++) {
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
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());

  ++it;
  ASSERT_FALSE(it == cube->end());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
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
    cube->process(NULL, t);
  }

  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 5; i++) {
    js_usleep(1000000);
  }

  jetstream::Tuple max;
  max.add_e(); //time
  max.add_e(); //url
  max.add_e(); //rc

  list<string> sort;
  sort.push_back("response_code");

  CubeIterator it = cube->slice_query(max, max, true, sort);
  ASSERT_EQ((size_t)5, it.numCells());

  boost::shared_ptr<Tuple> ptrTup;
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(100, ptrTup->e(2).i_val());

  it = cube->slice_query(max, max, true, sort, 1);
  ASSERT_EQ((size_t)1, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(100, ptrTup->e(2).i_val());

  sort.clear();
  sort.push_back("-response_code");

  it = cube->slice_query(max, max, true, sort);
  ASSERT_EQ((size_t)5, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(500, ptrTup->e(2).i_val());

  it = cube->slice_query(max, max, true, sort, 3);
  ASSERT_EQ((size_t)3, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(500, ptrTup->e(2).i_val());

}

TEST(Cube,Attach) {
  int compID = 4;

  NodeConfig cfg;
  boost::system::error_code error;
  Node node(cfg, error);
  node.start();
  ASSERT_TRUE(error == 0);

  AlterTopo topo;
  topo.set_computationid(compID);
  string cubeName = "text_and_count";

  jetstream::CubeMeta * cube_meta = topo.add_tocreate();
  cube_meta->set_name(cubeName);
  cube_meta->set_overwrite_old(true);

  jetstream::CubeSchema * sc = cube_meta->mutable_schema();

  jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
  dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
  dim->set_name("text");
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  agg->add_tuple_indexes(1);

  unsigned int K = 5;
  TaskMeta* task =  add_operator_to_alter(topo, operator_id_t(compID, 1), "SendK");
  add_cfg_to_task(task, "k", boost::lexical_cast<string>(K));
  add_cfg_to_task(task, "send_now","true");

  Edge * e = topo.add_edges();
  e->set_src(1);
  e->set_dest_cube(cubeName);
  e->set_computation(compID);

//  cout << topo.Utf8DebugString();

  ControlMessage r;
  node.handle_alter(topo, r);
  cout << "alter sent; data should be present" << endl;

  shared_ptr<DataCube> cube = node.get_cube(cubeName);
  ASSERT_TRUE( cube.get() );

  for(int i =0; i < 20 &&  cube->num_leaf_cells() < 1; i++) {
    js_usleep(100 * 1000);
  }

  ASSERT_EQ(1U, cube->num_leaf_cells());  //we sent several tuples that should have been aggregated
  Tuple empty = cube->empty_tuple();

  cube::CubeIterator it = cube->slice_query(empty, empty);

  for(int i =0; i < 20 &&  (it.numCells() < 1 || (*it)->e_size() < 2); i++) {
    js_usleep(100 * 1000);
    it = cube->slice_query(empty, empty);
  }

  ASSERT_EQ(1U, it.numCells());
  unsigned int total_count = 0;

  {  //scope to hide 't'
    shared_ptr<Tuple> t = *it;
    ASSERT_EQ(2, t->e_size()); //two elements; the dimension and the the count
    //ASSERT_EQ(K-1, t->version()); //k - 1

    total_count += t->e(1).i_val();
  }
  ASSERT_EQ(K, total_count);

  cube->clear_contents();
  ASSERT_EQ(0U, cube->num_leaf_cells());  //tests clear_contents

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
    cube->process(NULL, t);
  }

  for(int i =0; i < 5 &&  cube->num_leaf_cells() < 5; i++) {
    js_usleep(1000000);
  }

  jetstream::Element *e;
  jetstream::Tuple empty;
  e=empty.add_e(); //time
  e=empty.add_e(); //url
  e=empty.add_e(); //rc

  vector<unsigned int> levels;
  levels.push_back(MysqlDimensionTimeContainment::LEVEL_SECOND);
  levels.push_back(1);
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, empty, empty);
  ASSERT_EQ(1U, it.numCells());
  boost::shared_ptr<Tuple> ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  //ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_SECOND, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());
  //ASSERT_EQ(1, ptrTup->e(2).i_val());
  ASSERT_EQ(0, ptrTup->e(2).i_val());
  //ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(15, ptrTup->e(3).i_val());
  ASSERT_EQ(100, ptrTup->e(4).i_val());


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


TEST_F(CubeTest, MysqlTestTimeContainmentRollup) {
  jetstream::CubeSchema schema;
  jetstream::CubeSchema_Dimension * dim = schema.add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
  dim->add_tuple_indexes(0);

  dim = schema.add_dimensions();
  dim->set_name("url");
  dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
  dim->add_tuple_indexes(1);

  dim = schema.add_dimensions();
  dim->set_name("response_code");
  dim->set_type(CubeSchema_Dimension_DimensionType_INT32);
  dim->add_tuple_indexes(2);

  jetstream::CubeSchema_Aggregate * agg = schema.add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  agg->add_tuple_indexes(4);

  agg = schema.add_aggregates();
  agg->set_name("avg_size");
  agg->set_type("avg");
  agg->add_tuple_indexes(3);
  agg->add_tuple_indexes(5);

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(schema, "web_requests", true);

  cube->destroy();
  cube->create();

  jetstream::Element *e;

  //make it even on the minute
  time_t time_entered = time(NULL);
  struct tm temptm;
  gmtime_r(&time_entered, &temptm);
  temptm.tm_sec=0;
  time_entered = timegm(&temptm);

  list<int> rscs;
  rscs.push_back(1);
  rscs.push_back(2);
  rscs.push_back(60*60+1);
  rscs.push_back(60*60+1);

  for(list<int>::iterator i = rscs.begin(); i!=rscs.end(); i++) {
    boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
    insert_tuple(*t, time_entered+(*i), "http:\\\\www.example.com", *i, 500, 100);
    cube->process(NULL, t);
  }

  cube->wait_for_commits();

  jetstream::Tuple empty;
  e=empty.add_e(); //time
  e=empty.add_e(); //url
  e=empty.add_e(); //rc

  jetstream::Tuple max;
  e=max.add_e(); //time
  e->set_t_val(time_entered+1);
  e=max.add_e(); //url
  e=max.add_e(); //rc

  vector<unsigned int> levels;
  levels.push_back(MysqlDimensionTimeContainment::LEVEL_SECOND);
  levels.push_back(1);
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, max, max);
  ASSERT_EQ(1U, it.numCells());
  boost::shared_ptr<Tuple> ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(time_entered+1, ptrTup->e(0).t_val());
  //ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_SECOND, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());
  //ASSERT_EQ(1, ptrTup->e(2).i_val());
  ASSERT_EQ(0, ptrTup->e(2).i_val());
  //ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(100, ptrTup->e(3).i_val());
  ASSERT_EQ(5, ptrTup->e(4).i_val());

  levels.clear();
  levels.push_back(MysqlDimensionTimeContainment::LEVEL_SECOND-1);//5sec
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
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(time_entered, ptrTup->e(0).t_val());
  //ASSERT_EQ((int)MysqlDimensionTimeHierarchy::LEVEL_MINUTE, ptrTup->e(1).i_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());
  //ASSERT_EQ(1, ptrTup->e(2).i_val());
  ASSERT_EQ(0, ptrTup->e(2).i_val());
  //ASSERT_EQ(0, ptrTup->e(4).i_val());
  ASSERT_EQ(200, ptrTup->e(3).i_val());
  ASSERT_EQ(5, ptrTup->e(4).i_val());

  //test full rollup
  levels.clear();
  levels.push_back(0);
  levels.push_back(1);
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  it = cube->rollup_slice_query(levels, empty, empty);
  ASSERT_EQ(1U, it.numCells());
  ptrTup = *it;
  ASSERT_TRUE(ptrTup.get());
  ASSERT_EQ(0, ptrTup->e(0).t_val());
  ASSERT_STREQ("http:\\\\www.example.com", ptrTup->e(1).s_val().c_str());
  ASSERT_EQ(0, ptrTup->e(2).i_val());
  ASSERT_EQ(400, ptrTup->e(3).i_val());
  ASSERT_EQ(5, ptrTup->e(4).i_val());

}

TEST_F(CubeTest, MysqlTestHistogram) {
  jetstream::CubeSchema schema;
  jetstream::CubeSchema_Dimension * dim = schema.add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = schema.add_aggregates();
  agg->set_name("responses");
  agg->set_type("quantile_histogram");
  agg->add_tuple_indexes(1);

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(schema, "quantiles_histo", true);

  cube->destroy();
  cube->create();

  time_t time_entered = time(NULL);
  jetstream::Element *e;

  for (int j = 0; j< 3; ++j ) {
    boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
    e=t->add_e();
    e->set_t_val(time_entered+j);
    LogHistogram histo(30);
    const int ITEMS = 20;

    for(int i = 0; i < ITEMS; ++i) {
      histo.add_item(i+(10*j), 2);
      //histo.add_item(i*i+j, 1);
    }

    e = t->add_e();
    JSSummary * summary = e->mutable_summary();
    histo.serialize_to(*summary);
    cube->process(NULL, t);
  }

  cube->wait_for_commits();

  jetstream::Tuple empty;
  e=empty.add_e(); //time

  vector<unsigned int> levels;
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, empty, empty);
  ASSERT_EQ(1U, it.numCells());

  boost::shared_ptr<Tuple> ptrTup = *it;
  const JSSummary &sum_res = ptrTup->e(1).summary();

  LogHistogram res(sum_res);
  ASSERT_EQ(120U, res.pop_seen());

}

TEST_F(CubeTest, MysqlTestReservoirSampleAggregate) {
  //test aggregate function

  jetstream::CubeSchema schema;
  jetstream::CubeSchema_Dimension * dim = schema.add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = schema.add_aggregates();
  agg->set_name("responses");
  agg->set_type("quantile_sample");
  agg->add_tuple_indexes(1);

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(schema, "quantiles_sample", true);

  cube->destroy();
  cube->create();

  time_t time_entered = time(NULL);
  jetstream::Element *e;

  int nominal_sum = 0;
  for (int j = 0; j< 2; ++j ) {
    boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
    e=t->add_e();
    e->set_t_val(time_entered+j);
    const int ITEMS = 20;

    ReservoirSample agg(ITEMS);

    for(int i = 0; i < ITEMS; ++i) {
      agg.add_item(i+(10*j), 1);
      nominal_sum +=  (i + 10*j);
    }
    ASSERT_EQ(9 + 10*j, (int) agg.mean());

    e = t->add_e();
    JSSummary * summary = e->mutable_summary();

    agg.serialize_to(*summary);
    cube->process(NULL, t);
  }

  cube->wait_for_commits();

  jetstream::Tuple empty;
  e=empty.add_e(); //time

  vector<unsigned int> levels;
  levels.push_back(0);

  cube->do_rollup(levels, empty, empty);
  CubeIterator it = cube->rollup_slice_query(levels, empty, empty);
  ASSERT_EQ(1U, it.numCells());

  boost::shared_ptr<Tuple> ptrTup = *it;
  const JSSummary &sum_res = ptrTup->e(1).summary();

  ReservoirSample res(sum_res);
  ASSERT_EQ(40U, res.pop_seen());
  cout << "nominal sum is " << nominal_sum << "." <<endl;
  cout << "sample mean was " << res.mean()<<", should be " << (nominal_sum / 40.0) << endl;
  ASSERT_EQ(13, (int) res.mean());
}

TEST_F(CubeTest, MysqlTestReservoirSamplePair) {
  //test aggregate function

  jetstream::CubeSchema schema;
  jetstream::CubeSchema_Dimension * dim = schema.add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = schema.add_aggregates();
  agg->set_name("responses");
  agg->set_type("quantile_sample");
  agg->add_tuple_indexes(1);

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(schema, "quantiles_sample", true);

  cube->destroy();
  cube->create();

  time_t time_entered = time(NULL);
  jetstream::Element *e;

  int true_sum = 0;
  for (int j = 0; j< 2; ++j ) {
    boost::shared_ptr<jetstream::Tuple> t = boost::make_shared<jetstream::Tuple>();
    e=t->add_e();
    e->set_t_val(time_entered);
    ReservoirSample agg(30);
    const int ITEMS = 20;

    for(int i = 0; i < ITEMS; ++i) {
      agg.add_item(i+ ITEMS*j, 1);  //interval from 0 - ITEMS and ITEMS to 2 * ITEMS -1
      true_sum += ( i + ITEMS*j);
    }

    e = t->add_e();
    JSSummary * summary = e->mutable_summary();

    agg.serialize_to(*summary);
    cube->process(NULL, t);

    cube->wait_for_commits();
  }

  cube->wait_for_commits();

  jetstream::Tuple empty;
  e=empty.add_e(); //time

  CubeIterator it = cube->slice_query(empty, empty);
  ASSERT_EQ(1U, it.numCells());

  boost::shared_ptr<Tuple> ptrTup = *it;
  const JSSummary &sum_res = ptrTup->e(1).summary();

  ReservoirSample res(sum_res);
//  cout << "reservoir is " << res << endl;
  ASSERT_EQ(40U, res.pop_seen());
  cout << "true mean is " << (true_sum / 40.0) << endl;
  ASSERT_EQ(13, (int) res.mean());

}



TEST_F(CubeTest, Aggregates) {

  sc = new jetstream::CubeSchema();

  jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
  dim->add_tuple_indexes(0);

  jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  agg->add_tuple_indexes(1);

  agg = sc->add_aggregates();
  agg->set_name("avg_size");
  agg->set_type("avg");
  agg->add_tuple_indexes(2);
  agg->add_tuple_indexes(3);

  agg = sc->add_aggregates();
  agg->set_name("min_i");
  agg->set_type("min_i");
  agg->add_tuple_indexes(4);

  agg = sc->add_aggregates();
  agg->set_name("min_d");
  agg->set_type("min_d");
  agg->add_tuple_indexes(5);

  agg = sc->add_aggregates();
  agg->set_name("min_t");
  agg->set_type("min_t");
  agg->add_tuple_indexes(6);

  MysqlCube * cube = new MysqlCube(*sc, "agg_tests", true);

  boost::shared_ptr<Aggregate> vers_agg = cube->get_aggregate("version");
  ASSERT_TRUE( vers_agg != NULL );

  //  cube->destroy();
  cube->create();

  jetstream::Tuple t;
//  t.set_version(0);  //shouldn't be needed
  time_t time_entered = time(NULL);

  t.clear_e();
  jetstream::Element *e = t.add_e();
  e->set_t_val(time_entered);  //0 - time
  e=t.add_e();
  e->set_i_val(1);  //1 - count
  e=t.add_e();
  e->set_i_val(10);  //2- avg_sum
  e=t.add_e();
  e->set_i_val(1);  //3- avg_count
  e=t.add_e();
  e->set_i_val(1);  //4 - min-i
  e=t.add_e();
  e->set_d_val((double) 1);  //5 - min-d
  e=t.add_e();
  e->set_t_val((time_t) 1);  //6 - min-t

  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  try {
    cube->save_tuple(t, true, false, new_tuple, old_tuple);
  } catch (sql::SQLException &e) {
    LOG(FATAL) << e.what();
  }
  t.clear_e();
  e = t.add_e();
  e->set_t_val(time_entered);  //0 - time
  e=t.add_e();
  e->set_i_val(2);  //1 - count
  e=t.add_e();
  e->set_i_val(10);  //2- avg_sum
  e=t.add_e();
  e->set_i_val(2);  //3- avg_count
  e=t.add_e();
  e->set_i_val(2);  //4 - min-i
  e=t.add_e();
  e->set_d_val((double) 2);  //5 - min-d
  e=t.add_e();
  e->set_t_val((time_t) 2);  //6 - min-t

  cube->save_tuple(t, true, false, new_tuple, old_tuple);

  ASSERT_EQ(time_entered, new_tuple->e(0).t_val());
  ASSERT_EQ(3, new_tuple->e(1).i_val());
  ASSERT_EQ(20, new_tuple->e(2).i_val());
  ASSERT_EQ(3, new_tuple->e(3).i_val());
  ASSERT_EQ(1, new_tuple->e(4).i_val());
  ASSERT_EQ((double)1, new_tuple->e(5).d_val());
  ASSERT_EQ((time_t)1, new_tuple->e(6).t_val());


}

/*
TEST_F(CubeTest, TimeRollupManager) {
boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);
cube->destroy();
cube->create();

jetstream::Element *e;

list<unsigned int> levels;
levels.push_back(MysqlDimensionTimeHierarchy::LEVEL_YEAR);
levels.push_back(1);
levels.push_back(1);

jetstream::Tuple empty;
e=empty.add_e(); //time
e=empty.add_e(); //url
e=empty.add_e(); //rc

TimeRollupManager trm(cube, levels, empty, empty, 0);

struct tm timetm;
timetm.tm_year = 101;
timetm.tm_mon = 1;
timetm.tm_mday = 1;
timetm.tm_hour = 2;
timetm.tm_min = 1;
timetm.tm_sec = 2;

time_t floor = trm.time_floor(timegm(&timetm));
time_t ciel = trm.time_ciel(timegm(&timetm));

struct tm floortm;
floortm.tm_year = 101;
floortm.tm_mon = 0;
floortm.tm_mday = 1;
floortm.tm_hour = 0;
floortm.tm_min = 0;
floortm.tm_sec = 0;


ASSERT_EQ(timegm(&floortm), floor);

struct tm cieltm;
cieltm.tm_year = 102;
cieltm.tm_mon = 0;
cieltm.tm_mday = 1;
cieltm.tm_hour = 0;
cieltm.tm_min = 0;
cieltm.tm_sec = 0;

ASSERT_EQ(timegm(&cieltm), ciel);

ASSERT_EQ(timegm(&cieltm), trm.time_ciel(timegm(&cieltm)));
ASSERT_EQ(timegm(&floortm), trm.time_floor(timegm(&floortm)));
}*/

TEST_F(CubeTest, PerChainRollupLevels) {
  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "web_requests", true);
  boost::shared_ptr<OperatorChain> chain1(new OperatorChain());
  boost::shared_ptr<OperatorChain> chain2(new OperatorChain());

  // Create a cube with two incoming chains; we don't need to construct the actual chains,
  // since the cube just treats them as opaque identifiers.
  cube->create();
  cube->add_chain(chain1);
  cube->add_chain(chain2);

  // Tell the cube that time data on one chain is rolled up to the minute level, and
  // for the other chain rolled up to the second level
  std::vector<boost::shared_ptr<jetstream::Tuple> > tuples;
  DataplaneMessage msg;
  msg.set_type(DataplaneMessage::ROLLUP_LEVELS);
  msg.add_rollup_levels(MysqlDimensionTimeContainment::LEVEL_SECOND);
  msg.add_rollup_levels(1);
  msg.add_rollup_levels(1);
  cube->process(chain1.get(), tuples, msg);
  msg.clear_rollup_levels();
  msg.add_rollup_levels(MysqlDimensionTimeContainment::LEVEL_MINUTE);
  msg.add_rollup_levels(1);
  msg.add_rollup_levels(1);
  cube->process(chain2.get(), tuples, msg);

  // Send the same tuple on each chain. Note that time data at the minute-level is still
  // reported in seconds, so it's the sender's responsibility to specify times at minute
  // boundaries, but we are violating that for the sake of testing.
  boost::shared_ptr<jetstream::Tuple> t1 = boost::make_shared<jetstream::Tuple>();
  time_t time_entered = time(NULL);
  insert_tuple(*t1, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  cube->process(chain1.get(), t1);
  cube->process(chain2.get(), t1);
  cube->wait_for_commits();

  // Check cube contents for the current minute
  jetstream::Element *e;
  jetstream::Tuple min;
  jetstream::Tuple max;
  e = min.add_e(); //time
  e->set_t_val(time_entered/60 * 60);
  e = min.add_e(); //url
  e = min.add_e(); //rc
  e = max.add_e(); //time
  e->set_t_val(time_entered + 60);
  e = max.add_e(); //url
  e = max.add_e(); //rc

  // A query of the raw data table should return two cells even though the same tuple was 
  // processed twice, since time data for one was interpreted at the minute level
  cube::CubeIterator it = cube->slice_query(min, max, false);
  ASSERT_EQ(2U, it.numCells());

  // A rollup query should return nothing since although we received pre-rolled up data,
  // we didn't do a rollup ourselves
  vector<unsigned int> levels;
  levels.push_back(MysqlDimensionTimeContainment::LEVEL_MINUTE);
  levels.push_back(1);
  levels.push_back(1);
  it = cube->rollup_slice_query(levels, min, max, false);
  ASSERT_EQ(0U, it.numCells());

  // Now do a rollup and make sure the processed tuples are properly aggregated
  cube->do_rollup(levels, min, max);
  it = cube->rollup_slice_query(levels, min, max, false);
  ASSERT_EQ(1U, it.numCells());
  // Note that the returned time is at the minute boundary (even though the input wasn't)
  check_tuple(*it, time_entered / 60 * 60, "http:\\\\www.example.com", 200, 100, 2);
}
