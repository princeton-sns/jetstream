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
                   bool overwrite_if_present, const NodeConfig &conf): MysqlCube ( _schema, _name, overwrite_if_present, conf) {}

      virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) {}

};


class TestTupleGenerator {

  public:
    TestTupleGenerator(size_t num, DataCube * cube, unsigned int time_offset=0): cube(cube) {

      //time_t time_entered = time(NULL);
      int time_entered = 1;
      boost::shared_ptr<jetstream::Tuple> t;
  
      for(unsigned int i =0; i < num; i++) {
        t = boost::make_shared<jetstream::Tuple>();
        create_tuple(*t, time_entered+time_offset+i, "http:\\\\www.example.com", 200, 50, 1);
        tuples.push_back(t);
      }
      LOG(INFO) << "Generated "<< tuples.size() << " tuples. Num= "<<num << " Time started=" << time_entered << "Time ended" << (time_entered+num);

    }

    void create_tuple(jetstream::Tuple & t, time_t time, string url, int rc, int sum, int count) {
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

    void insert_into_cube() {
      unsigned int i = 0;
    
      ChainedQueueMonitor * procMon = ( ChainedQueueMonitor *)cube->congestion_monitor().get();
      QueueCongestionMonitor * flushMon =  (  QueueCongestionMonitor *)procMon->dest.get();


      for(std::vector< boost::shared_ptr<jetstream::Tuple> >::const_iterator it = tuples.begin(); it != tuples.end(); ++it) {
        cube->process(*it);
        ++i;

        if(i%100000 == 0)
          LOG(INFO) << "Insert into cube: outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
      }
    }

  protected:
  DataCube * cube;
    std::vector< boost::shared_ptr<jetstream::Tuple> > tuples;

};

class ProcessTest : public ::testing::Test {
  


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
/*
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

void make_tuples(std::vector< boost::shared_ptr<jetstream::Tuple> > & vector, unsigned int count, unsigned int rep)
{
  time_t time_entered = time(NULL);
  boost::shared_ptr<jetstream::Tuple> t;
  for(unsigned int i =0; i < count; i++) {
    t = boost::make_shared<jetstream::Tuple>();
    insert_tuple2(*t, time_entered+( i % rep ), "http:\\\\www.example.com", 200, 50, 1);
    vector.push_back(t);
  }
}*/

void run_test(jetstream::CubeSchema * sc, bool use_db, unsigned int num_tuples, size_t num_tuple_insert_threads, size_t num_process_threads, bool overlap = true) {
  NodeConfig conf;
  conf.cube_processor_threads = num_process_threads;
  
  LOG(INFO) << "Running Test " << (use_db? "with db": "withOUT DB") << " num_tuples: "<< num_tuples << " num insert threads: "<< num_tuple_insert_threads<< " num process threads: "<< num_process_threads ;

  MysqlCube * cube;

  if(use_db) {
    cube = new MysqlCube(*sc, "web_requests", true, conf);
  }
  else {
    cube = new MysqlCubeNoDB(*sc, "web_requests", true, conf);
  }

  cube->destroy();
  cube->create();

  std::vector< TestTupleGenerator * > gens;

  for(size_t i = 0; i<num_tuple_insert_threads; ++i) {
    TestTupleGenerator * g;
    if(overlap)
     g = new TestTupleGenerator(num_tuples/num_tuple_insert_threads, cube);
    else
     g = new TestTupleGenerator(num_tuples/num_tuple_insert_threads, cube, (num_tuples/num_tuple_insert_threads)*i);
    gens.push_back(g);
  }

  msec_t start = get_msec();

  LOG(INFO) << "starting timer: "<< start;

  js_usleep(5000);
  
  boost::thread_group tg;
  for(size_t i = 0; i<gens.size(); ++i) {
    TestTupleGenerator * g= gens[i];
    boost::thread *t1 = new boost::thread(&TestTupleGenerator::insert_into_cube, g);
    tg.add_thread(t1);
  }

  tg.join_all();

  ChainedQueueMonitor * procMon = ( ChainedQueueMonitor *)cube->congestion_monitor().get();
  QueueCongestionMonitor * flushMon =  (  QueueCongestionMonitor *)procMon->dest.get();



  int waits = 0;

  while(procMon->queue_length() > 0 || flushMon->queue_length() > 0) {
    waits ++;
    js_usleep(200000);
    LOG(INFO) << "Waiting on completeness. outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
  }

  LOG(INFO) << "Outstanding " << procMon->queue_length() <<"; waits "<< waits << "; start" << start << "; now "<< get_msec();

  unsigned int diff =  (get_msec() - start);
  double rate = (double) num_tuples/diff;

  LOG(INFO) << "Finished Test " << (use_db? "with db": "withOUT DB") << " num_tuples: "<< num_tuples << " num insert threads: "<< num_tuple_insert_threads<< " num process threads: "<< num_process_threads << " Overlap "<< overlap <<". The time it took was: " << diff <<" ms. Rate = " << rate <<" tuples/ms";

}

TEST_F(ProcessTest, DISABLED_ND1M22) {
  run_test(sc, false, 1000000, 2, 2);
}

TEST_F(ProcessTest, DISABLED_ND1M12) {
  run_test(sc, false, 1000000, 1, 1);
}

TEST_F(ProcessTest, DISABLED_D1M44) {
  run_test(sc, true, 1000000, 4, 4);
}

TEST_F(ProcessTest, DISABLED_D10044) {
  run_test(sc, true, 100, 4, 4);
}

TEST_F(ProcessTest, DISABLED_D10K44) {
  run_test(sc, true, 10000, 4, 4);
}

TEST_F(ProcessTest, DISABLED_D100K44O) {
  run_test(sc, true, 100000, 4, 4, true);
}

TEST_F(ProcessTest, DISABLED_D100K11NO) {
  run_test(sc, true, 100000, 1, 1, false);
}

TEST_F(ProcessTest, DISABLED_D100K44NO) {
  run_test(sc, true, 100000, 4, 4, false);
}

TEST_F(ProcessTest, DISABLED_D500K44NO) {
  run_test(sc, true, 500000, 4, 4, false);
}

TEST_F(ProcessTest, DISABLED_D1M44NO) {
  run_test(sc, true, 1000000, 4, 4, false);
}

TEST_F(ProcessTest, DISABLED_D100K88NO) {
  run_test(sc, true, 100000, 8, 8, false);
}


TEST_F(ProcessTest, DISABLED_D100K14) {
  run_test(sc, true, 100000, 1, 4);
}

TEST_F(ProcessTest, DISABLED_D100K41) {
  run_test(sc, true, 100000, 4, 1);
}

TEST_F(ProcessTest, DISABLED_D100K11) {
  run_test(sc, true, 100000, 1, 1);
}

TEST_F(ProcessTest, DISABLED_D10K11) {
  run_test(sc, true, 10000, 1, 1);
}

TEST_F(ProcessTest, DISABLED_D1M22) {
  run_test(sc, true, 1000000, 2, 2);
}

TEST_F(ProcessTest, DISABLED_D1M12) {
  run_test(sc, true, 1000000, 1, 2);
}

TEST_F(ProcessTest, DISABLED_D1M21) {
  run_test(sc, true, 1000000, 2, 1);
}

TEST_F(ProcessTest, DISABLED_D1M11) {
  run_test(sc, true, 1000000, 1, 1);
}

TEST_F(ProcessTest, DISABLED_ND200K22) {
  run_test(sc, false, 200000, 2, 2);
}

TEST_F(ProcessTest, DISABLED_ND1M11) {
  run_test(sc, false, 1000000, 1, 1);
}

/*
TEST_F(ProcessTest, LoopTest) {
  MysqlCubeNoDB * cube = new MysqlCubeNoDB(*sc, "web_requests", true);
  boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  //cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  ChainedQueueMonitor * procMon = ( ChainedQueueMonitor *)cube->congestion_monitor().get();
  QueueCongestionMonitor * flushMon =  (  QueueCongestionMonitor *)procMon->dest.get();
 
  std::vector< boost::shared_ptr<jetstream::Tuple> > vector;
  make_tuples(vector, 1000000, 100);

  unsigned int i = 0;

  msec_t start = get_msec();
  for(std::vector< boost::shared_ptr<jetstream::Tuple> >::const_iterator it = vector.begin(); it != vector.end(); ++it)
  {
    cube->process(*it);
    ++i;
    if(i%100000 == 0)
      LOG(INFO) << "Outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
  }

  int waits = 0;
  while(procMon->queue_length() > 0 || flushMon->queue_length() > 0)
  {
    waits ++;
    js_usleep(200000);
    LOG(INFO) << "Outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
  }
  
  LOG(INFO) << "Outstanding " << procMon->queue_length() <<"; waits "<< waits;

  LOG(INFO) << "The time it took was: " << (get_msec() - start);
  //js_usleep(200000);
  //delete cube;
}

TEST_F(ProcessTest, LoopWithDbTest) {
  MysqlCube * cube = new MysqlCube(*sc, "web_requests", true);
  //boost::shared_ptr<cube::QueueSubscriber> sub= make_shared<cube::QueueSubscriber>();
  //cube->add_subscriber(sub);
  cube->destroy();
  cube->create();

  ChainedQueueMonitor * procMon = ( ChainedQueueMonitor *)cube->congestion_monitor().get();
  QueueCongestionMonitor * flushMon =  (  QueueCongestionMonitor *)procMon->dest.get();
  
  std::vector< boost::shared_ptr<jetstream::Tuple> > vector;
  make_tuples(vector, 1000000, 100);

  unsigned int i = 0;

  msec_t start = get_msec();
  for(std::vector< boost::shared_ptr<jetstream::Tuple> >::const_iterator it = vector.begin(); it != vector.end(); ++it)
  {
    cube->process(*it);
    ++i;
    if(i%100000 == 0)
      LOG(INFO) << "Outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
  }

  int waits = 0;

  while(procMon->queue_length() > 0 || flushMon->queue_length() > 0)
  {
    waits ++;
    LOG(INFO) << "Outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length() <<"; waits "<< waits;
    js_usleep(200000);
  }

  LOG(INFO) << "Outstanding " << procMon->queue_length() <<"; waits "<< waits;
  LOG(INFO) << "The time it took was: " << (get_msec() - start);
  //js_usleep(200000);

  //delete cube;
}*/

TEST_F(ProcessTest, DISABLED_KeyTest) {
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
TEST_F(ProcessTest, DISABLED_Key2Test) {
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

TEST_F(ProcessTest, DISABLED_Key3Test) {
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

TEST_F(ProcessTest, DISABLED_Key4Test) {
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
