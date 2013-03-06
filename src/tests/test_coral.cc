#include "js_utils.h"

#include "cube_manager.h"
#include "node.h"

#include "mysql_cube.h"
#include "base_subscribers.h"
#include "cube_iterator_impl.h"

#include "base_operators.h"
#include "experiment_operators.h"
#include "summary_operators.h"
#include "quantile_est.h"
//#include "time_rollup_manager.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace jetstream::cube;
using namespace boost;

const string CORAL_FILE =  "src/tests/data/coral.log";

class TestTupleCoralGenerator {

  public:

    void configure_operator(shared_ptr<DataPlaneOperator> d,  operator_config_t cfg)
    {
        operator_err_t err = d->configure(cfg);
        if (err != NO_ERR) {
          LOG(FATAL) << "Config Error: " <<err;  
        }
    }

    TestTupleCoralGenerator(size_t num,  boost::shared_ptr<MysqlCube> cube): cube(cube) {

      //time_t time_entered = time(NULL);
      //int time_entered = 1;
      //boost::shared_ptr<jetstream::Tuple> t;
  
        shared_ptr<FileRead> fr_op(new FileRead);
        shared_ptr<CSVParse> csvp_op(new CSVParseStrTk);
        shared_ptr<ExperimentTimeRewrite> tr_op(new ExperimentTimeRewrite);
        
        operator_config_t cfg;
        cfg["file"] = CORAL_FILE;
        cfg["skip_empty"] = "true";
        cfg["discard_off_size"] = "true";
        cfg["types"] = "IDSSSSSSIIIIIIISS";
        cfg["fields_to_keep"]="all";
        //cfg["fld_offset"]="1";
        //cfg["round_to"]="1";
        //cfg["in_type"]="D";
        //cfg["add_offset"]="0";
        

        cfg["field"]="1";
        cfg["warp"]="300";

        configure_operator(fr_op, cfg);
        configure_operator(csvp_op, cfg);
        configure_operator(tr_op, cfg);
        
        shared_ptr<ToSummary> summary_nbytes_op(new ToSummary);
        operator_config_t cfg_sum_nbytes;
        cfg_sum_nbytes["size"] = "100";
        cfg_sum_nbytes["field"] = "9"; //nbytes
        configure_operator(summary_nbytes_op, cfg_sum_nbytes);

        shared_ptr<ToSummary> summary_dl_time_op(new ToSummary);
        operator_config_t cfg_sum_dl_time;
        cfg_sum_dl_time["size"] = "100";
        cfg_sum_dl_time["field"] = "14"; //dl_time
        configure_operator(summary_dl_time_op, cfg_sum_dl_time);


        fr_op->set_dest(csvp_op);
        csvp_op->set_dest(tr_op);
        tr_op->set_dest(summary_nbytes_op);
        summary_nbytes_op->set_dest(summary_dl_time_op);

        //summary_dl_time_op->set_dest(cube);
        shared_ptr<DummyReceiver> receive(new DummyReceiver);
        summary_dl_time_op->set_dest(receive);

      while(tuples.size() < num) {
        receive->tuples.clear();
        fr_op->emit_1();

        tuples.insert(tuples.end(), receive->tuples.begin(), receive->tuples.end());
        
      
        //t = boost::make_shared<jetstream::Tuple>();
        //create_tuple(*t, time_entered+time_offset+i, "http:\\\\www.example.com", 200, 50, 1);
        //tuples.push_back(t);
      }
      LOG(INFO) << "Read " << tuples.size() <<" tuples. ";
      //Tuples have "<<(*(tuples.begin()))->e_size() << " Elements. "<<fmt(**(tuples.begin()));
      //LOG(INFO) << "Generated "<< tuples.size() << " tuples. Num= "<<num << " Time started=" << time_entered << "Time ended" << (time_entered+num);

    }

    void insert_into_cube() {
      unsigned int i = 0;
    
      ChainedQueueMonitor * procMon = ( ChainedQueueMonitor *)cube->congestion_monitor().get();
      QueueCongestionMonitor * flushMon =  (  QueueCongestionMonitor *)procMon->dest.get();


      for(std::vector< boost::shared_ptr<jetstream::Tuple> >::const_iterator it = tuples.begin(); it != tuples.end(); ++it) {
        //LOG(INFO) <<"Inserting: "<< fmt(*(*it));
        cube->process(*it);
        ++i;

        if(i%100000 == 0)
          LOG(INFO) << "Insert into cube: outstanding process " << procMon->queue_length() <<" outstanding flush " << flushMon->queue_length();
      }
      LOG(INFO) << "Finished inserting into cube: " << i << "tuples";
    }

  protected:
  boost::shared_ptr<MysqlCube> cube;
  std::vector< boost::shared_ptr<jetstream::Tuple> > tuples;

};

class TestCSVGenerator {

  public:

    void configure_operator(shared_ptr<DataPlaneOperator> d,  operator_config_t cfg)
    {
        operator_err_t err = d->configure(cfg);
        if (err != NO_ERR) {
          LOG(FATAL) << "Config Error: " <<err;  
        }
    }

    TestCSVGenerator(size_t num) {

        shared_ptr<FileRead> fr_op(new FileRead);
        operator_config_t cfg;
        cfg["file"] = CORAL_FILE;
        cfg["skip_empty"] = "true";

        configure_operator(fr_op, cfg);
        
        shared_ptr<DummyReceiver> receive(new DummyReceiver);
        fr_op->set_dest(receive);
        
        while(tuples.size() < num) {
          fr_op->emit_1();
          tuples.insert(tuples.end(), receive->tuples.begin(), receive->tuples.end());
      }
      LOG(INFO) << "Read " << tuples.size() <<" tuples.";
    }

    void parse() {
      unsigned int i = 0;

      shared_ptr<CSVParse> csvp_op(new CSVParseStrTk);

      operator_config_t cfg;
      cfg["discard_off_size"] = "true";
      cfg["types"] = "IDSSSSSSIIIIIIISS";
      cfg["fields_to_keep"]="all";

      configure_operator(csvp_op, cfg);

      shared_ptr<DummyReceiver> receive(new DummyReceiver);
      csvp_op->set_dest(receive);
      for(std::vector< boost::shared_ptr<jetstream::Tuple> >::const_iterator it = tuples.begin(); it != tuples.end(); ++it) {
        //LOG(INFO) <<"Inserting: "<< fmt(*(*it));
        csvp_op->process(*it);
        ++i;

        LOG_EVERY_N(INFO, 10000) << "CSV Parse at " << i <<"Tup " << fmt(*(receive->tuples[0]));
      }
      LOG(INFO) << "Finished parsing: " << i << "tuples";
    }

  protected:
  std::vector< boost::shared_ptr<jetstream::Tuple> > tuples;

};









class CoralTest : public ::testing::Test {
  


  protected:
    virtual void SetUp() {

      sc = new jetstream::CubeSchema();

      jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
      dim->set_name("time");
      dim->set_type(CubeSchema_Dimension_DimensionType_TIME_CONTAINMENT);
      dim->add_tuple_indexes(1);

      dim = sc->add_dimensions();
      dim->set_name("url");
      dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
      dim->add_tuple_indexes(6);

      dim = sc->add_dimensions();
      dim->set_name("response_code");
      dim->set_type(CubeSchema_Dimension_DimensionType_INT32);
      dim->add_tuple_indexes(8);

      jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
      agg->set_name("nbytes");
      agg->set_type("quantile_histogram");
      agg->add_tuple_indexes(9);

      agg = sc->add_aggregates();
      agg->set_name("dl_time");
      agg->set_type("quantile_histogram");
      agg->add_tuple_indexes(14);

      agg = sc->add_aggregates();
      agg->set_name("count");
      agg->set_type("count");
      agg->add_tuple_indexes(17);
      
      MysqlCube::set_db_params("localhost", "root", "", "test_cube");
    }

    jetstream::CubeSchema * sc;


    virtual void TearDown() {
      delete sc;
    }
};

void run_test(jetstream::CubeSchema * sc,  unsigned int num_tuples, size_t num_tuple_insert_threads, size_t num_process_threads) {
  NodeConfig conf;
  conf.cube_processor_threads = num_process_threads;
  
  LOG(INFO) << "Running Test num_tuples: "<< num_tuples << " num insert threads: "<< num_tuple_insert_threads<< " num process threads: "<< num_process_threads ;

  boost::shared_ptr<MysqlCube> cube = boost::make_shared<MysqlCube>(*sc, "coral_tests", true, conf);

  cube->destroy();
  cube->create();

  std::vector< TestTupleCoralGenerator * > gens;

  for(size_t i = 0; i<num_tuple_insert_threads; ++i) {
    TestTupleCoralGenerator * g;
    g = new TestTupleCoralGenerator(num_tuples/num_tuple_insert_threads, cube);
    gens.push_back(g);
  }

  msec_t start = get_msec();

  LOG(INFO) << "starting timer: "<< start;

  boost::thread_group tg;
  for(size_t i = 0; i<gens.size(); ++i) {
    TestTupleCoralGenerator * g= gens[i];
    boost::thread *t1 = new boost::thread(&TestTupleCoralGenerator::insert_into_cube, g);
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

  LOG(INFO) << "Finished Test num_tuples: "<< num_tuples << " num insert threads: "<< num_tuple_insert_threads<< " num process threads: "<< num_process_threads << ". The time it took was: " << diff << " ms. Rate="<<rate<<" tuples/ms";

}

void run_csv_test(unsigned int num_tuples) {
  
  LOG(INFO) << "Running CSV Test num_tuples: "<< num_tuples;

  TestCSVGenerator * g;
  g = new TestCSVGenerator(num_tuples);

  msec_t start = get_msec();

  LOG(INFO) << "starting timer: "<< start;

  g->parse();

  unsigned int diff =  (get_msec() - start);
  double rate = (double) num_tuples/diff;

  LOG(INFO) << "Finished Test num_tuples: "<< num_tuples << ". The time it took was: " << diff <<" ms. Rate = "<<rate<<" tuples/ms";

}



TEST_F(CoralTest, DISABLED_2K14) {
  run_test(sc, 2000, 1, 4);
}

TEST_F(CoralTest, DISABLED_20K14) {
  run_test(sc, 20000, 1, 4);
}


TEST_F(CoralTest, DISABLED_CSV20K) {
  run_csv_test(20000);
}

TEST_F(CoralTest, DISABLED_CSV200K) {
  run_csv_test(200000);
}
