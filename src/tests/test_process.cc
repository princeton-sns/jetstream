#include "js_utils.h"

#include "cube_manager.h"
#include "node.h"

#include "mysql_cube.h"
#include "base_subscribers.h"
#include "cube_iterator_impl.h"
//#include "time_rollup_manager.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;
  class MysqlCubeNoDB: public MysqlCube {
   
    public:

    MysqlCubeNoDB (jetstream::CubeSchema const _schema,
               string _name,
               bool overwrite_if_present,
               size_t batch=1): MysqlCube ( _schema, _name, overwrite_if_present, batch) {}

    virtual void do_flush(boost::shared_ptr<cube::TupleBatch> tb) {
      flushCongestMon->report_delete(tb.get(), 1);
    }

    int outstanding_processes()
    {
      return processCongestMon->queue_length();
    }
};


class ProcessTest : public ::testing::Test {
  


  protected:
    virtual void SetUp() {

      sc = new jetstream::CubeSchema();

      jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
      dim->set_name("time");
      dim->set_type(CubeSchema_Dimension_DimensionType_TIME_HIERARCHY);
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

void insert_tuple2(jetstream::Tuple & t, time_t time, string url, int rc, int sum, int count) {
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

TEST_F(ProcessTest, LoopTest) {
  MysqlCubeNoDB * cube = new MysqlCubeNoDB(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  cube->set_elements_in_batch(20);
  //cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  
  time_t time_entered = time(NULL);
  boost::shared_ptr<jetstream::Tuple> t;
  t = boost::make_shared<jetstream::Tuple>();
  insert_tuple2(*t, time_entered, "http:\\\\www.example.com", 200, 50, 1);
  for(int i =0; i < 1000000; i++) {
  //t = boost::make_shared<jetstream::Tuple>();
  //insert_tuple2(*t, time_entered+i, "http:\\\\www.example.com", 200, 50, 1);
    cube->process(t);
  }

  int waits = 0;
  while(cube->outstanding_processes()> 0)
  {
    waits ++;
    js_usleep(200000);
  }

  LOG(INFO) << "Outstanding " << cube->outstanding_processes()<<"; waits "<< waits;
  //js_usleep(200000);

  //delete cube;
}

TEST_F(ProcessTest, KeyTest) {
  //time, string, int


  time_t t = time(NULL);
  string s = "http:\\\\www.example.com";
  int in = 200; 
  string res;
  for(int i =0; i < 1000000; i++) {

    res = "";
    struct tm temptm;
    char timestring[30];
    time_t clock = t;
    gmtime_r(&clock, &temptm);
    strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
   
    res += timestring;
    res +="|"+s+"|"+boost::lexical_cast<string>(in)+"|";
  }

}
TEST_F(ProcessTest, Key2Test) {
  //time, string, int


  time_t t = time(NULL);
  string s = "http:\\\\www.example.com";
  int in = 200; 
  string res;
  for(int i =0; i < 1000000; i++) {

    res = "";
    res += boost::lexical_cast<string>(t);
    res +="|"+s+"|"+boost::lexical_cast<string>(in)+"|";
  }

}

TEST_F(ProcessTest, Key3Test) {
  //time, string, int

  ostringstream test;
  test << "1";
  string s1 = test.str();
  test.str("");
  test.clear();
  test << "2";
  string s2 = test.str();
  LOG(INFO) << s1 << " != " << s2;



  time_t t = time(NULL);
  string s = "http:\\\\www.example.com";
  int in = 200; 
  string res;
  ostringstream st;
  for(int i =0; i < 1000000; i++) {
     st.str("");
     st.clear();
     st << t << "|" << s << "|" << in << "|";
     res=st.str();
  }

}

TEST_F(ProcessTest, Key4Test) {
  //time, string, int

  time_t t = time(NULL);
  string s = "http:\\\\www.example.com";
  int in = 200; 
  string res;
  for(int i =0; i < 1000000; i++) {
     ostringstream st;
     st << t << "|" << s << "|" << in << "|";
     res=st.str();
  }

}
